# This file is part of the Coyote <https://github.com/fpgasystems/Coyote>
#
# MIT Licence
# Copyright (c) 2025, Systems Group, ETH Zurich
# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# DEBUG (temporary): arms and reads out the debug ILAs of the lost RDMA WRITE investigation without the Vivado GUI
# (ila_dbg_cmd_queue in cnfg_slave_avx.sv, ila_dbg_rdma_wr_path in user_wrapper_tmplt.txt)
#
# Usage, on the machine whose FPGA is under test, after programming it and inserting the driver:
#   vivado -mode batch -nolog -nojournal -source ila_debug.tcl -tclargs arm    <cyt_top.ltx> [device index]
#   vivado -mode batch -nolog -nojournal -source ila_debug.tcl -tclargs upload <cyt_top.ltx> <out dir> [device index]
#
# arm:    stores only handshakes of pid DBG_PID (capture condition) and triggers on a drop condition or the watchdog marker.
#         The ILAs stay armed in hardware after Vivado exits.
# upload: writes the data of every ILA that has triggered as .csv and .ila (the latter can be opened in the GUI later).

if {[llength $argv] < 2} {
    puts "Usage: arm <ltx> \[device\] | upload <ltx> <out dir> \[device\]"
    exit 1
}

set mode [lindex $argv 0]
set ltx  [lindex $argv 1]
if {$mode eq "upload"} {
    set out_dir [lindex $argv 2]
    set dev_idx [expr {[llength $argv] > 3 ? [lindex $argv 3] : 0}]
} else {
    set dev_idx [expr {[llength $argv] > 2 ? [lindex $argv 2] : 0}]
}

# Samples kept after the trigger; the rest of the 4096 are the history before it
set trigger_position 4000

# ILA cell -> {capture probe, trigger probes}
set ilas {
    inst_ila_dbg_cmd_queue      {dbg_cmd_pid_hs     {dbg_cmd_overflow dbg_cmd_marker}}
    inst_ila_dbg_rdma_wr_path   {dbg_rdma_wr_pid_hs {dbg_rdma_wr_bad_opcode dbg_rdma_wr_bad_dest dbg_rdma_wr_marker}}
}

open_hw_manager
connect_hw_server -allow_non_jtag
open_hw_target

set dev [lindex [get_hw_devices] $dev_idx]
puts "Using hw device: $dev (all: [get_hw_devices])"
current_hw_device $dev
set_property PROBES.FILE $ltx $dev
set_property FULL_PROBES.FILE $ltx $dev
refresh_hw_device $dev

proc find_ila {dev cell} {
    set ila [get_hw_ilas -quiet -of_objects $dev -filter "CELL_NAME =~ *$cell"]
    if {[llength $ila] != 1} {
        puts "ERROR: expected one ILA matching $cell, found: '$ila' (all: [get_hw_ilas -of_objects $dev])"
        exit 1
    }
    return $ila
}

proc find_probe {ila name} {
    set probe [get_hw_probes -quiet -of_objects $ila -filter "NAME =~ *$name"]
    if {[llength $probe] != 1} {
        puts "ERROR: expected one probe matching $name in $ila, found: '$probe'"
        puts "Probes of $ila: [get_hw_probes -of_objects $ila]"
        exit 1
    }
    return $probe
}

if {$mode eq "arm"} {
    foreach {cell cfg} $ilas {
        set ila [find_ila $dev $cell]
        lassign $cfg capture_probe trigger_probes

        set_property CONTROL.DATA_DEPTH 4096 $ila
        set_property CONTROL.TRIGGER_POSITION $trigger_position $ila

        set_property CONTROL.CAPTURE_MODE BASIC $ila
        set_property CONTROL.CAPTURE_CONDITION AND $ila
        set_property CAPTURE_COMPARE_VALUE eq1'b1 [find_probe $ila $capture_probe]

        set_property CONTROL.TRIGGER_MODE BASIC_ONLY $ila
        set_property CONTROL.TRIGGER_CONDITION OR $ila
        foreach p $trigger_probes {
            set_property TRIGGER_COMPARE_VALUE eq1'b1 [find_probe $ila $p]
        }

        run_hw_ila $ila
        puts "Armed $ila: capture on $capture_probe, trigger on any of $trigger_probes"
    }
} elseif {$mode eq "upload"} {
    file mkdir $out_dir
    foreach {cell cfg} $ilas {
        set ila [find_ila $dev $cell]
        set status [get_property STATUS.CORE_STATUS $ila]
        puts "$ila: status $status"
        if {$status ne "FULL"} {
            puts "  not triggered yet, nothing to upload"
            continue
        }
        set data [upload_hw_ila_data $ila]
        write_hw_ila_data -force -csv_file "$out_dir/$cell.csv" $data
        write_hw_ila_data -force "$out_dir/$cell.ila" $data
        puts "  written $out_dir/$cell.csv and $out_dir/$cell.ila"
    }
} else {
    puts "Unknown mode $mode"
    exit 1
}

close_hw_target
disconnect_hw_server
close_hw_manager
