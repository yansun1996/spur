// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Shared exit-status / completion-reason rendering for the Slurm-compatible
//! CLIs (scontrol, squeue, sacct). Keeps the lossless `code:signal` encoding and
//! the `RaisedSignal:N(name)` reason composition in one place so the surfaces
//! cannot drift apart.

use spur_core::job::PendingReason;

/// Format an exit status as Slurm's `code:signal`.
pub fn format_exit(code: i32, signal: i32) -> String {
    format!("{}:{}", code, signal)
}

/// Human-readable name for a terminating signal (Slurm's `RaisedSignal` suffix).
pub fn signal_name(sig: i32) -> String {
    let name = match sig {
        1 => "Hangup",
        2 => "Interrupt",
        6 => "Aborted",
        9 => "Killed",
        11 => "Segmentation_fault",
        13 => "Broken_pipe",
        15 => "Terminated",
        _ => return format!("Signal {}", sig),
    };
    name.to_string()
}

/// Compose the displayed completion reason. For `RaisedSignal`, append
/// `:N(name)` from the job's terminating signal, matching Slurm
/// (`RaisedSignal:9(Killed)`). The wire value is compared against
/// `PendingReason::RaisedSignal.display()` so it tracks the enum, not a literal.
pub fn render_reason(reason: &str, signal: i32) -> String {
    if reason == PendingReason::RaisedSignal.display() && signal != 0 {
        format!("{}:{}({})", reason, signal, signal_name(signal))
    } else {
        reason.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_exit_code_pairs() {
        assert_eq!(format_exit(2, 0), "2:0");
        assert_eq!(format_exit(0, 9), "0:9");
        assert_eq!(format_exit(0, 0), "0:0");
    }

    #[test]
    fn signal_name_lookup() {
        assert_eq!(signal_name(9), "Killed");
        assert_eq!(signal_name(15), "Terminated");
        // Multi-word names use underscores to match Slurm (e.g. 25.11.6).
        assert_eq!(signal_name(11), "Segmentation_fault");
        assert_eq!(signal_name(13), "Broken_pipe");
        assert_eq!(signal_name(99), "Signal 99");
    }

    #[test]
    fn render_reason_composes_signal() {
        assert_eq!(render_reason("RaisedSignal", 9), "RaisedSignal:9(Killed)");
        assert_eq!(render_reason("NonZeroExitCode", 0), "NonZeroExitCode");
        assert_eq!(render_reason("None", 0), "None");
        // RaisedSignal without a signal stays bare (defensive).
        assert_eq!(render_reason("RaisedSignal", 0), "RaisedSignal");
    }
}
