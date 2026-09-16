// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! An agent restart between a ledger pull and its reply leaves the controller
//! holding a picture of a lifetime that has ended. Registration says which is current.

use std::collections::HashMap;

use parking_lot::Mutex;

/// Leader-local record of the agent lifetime each node last announced. Never
/// persisted: only this leader issued the pulls a record here can disown.
#[derive(Default)]
pub(crate) struct AgentSessions {
    current: Mutex<HashMap<String, String>>,
}

impl AgentSessions {
    /// Record the lifetime a node is registering under. Registration is the only
    /// writer: an agent announces a new lifetime by starting, and starting registers.
    pub(crate) fn observe_registration(&self, node: &str, session: &str) {
        if session.is_empty() {
            return;
        }
        self.current
            .lock()
            .insert(node.to_string(), session.to_string());
    }

    /// Whether a cut carrying `session` can still be read as this node's current
    /// state. Nothing recorded means no registration has replaced what it names.
    pub(crate) fn vouches_for(&self, node: &str, session: &str) -> bool {
        // Naming no lifetime is symmetric with not recording one: a cut that
        // cannot be told apart is left ungated rather than judged against another.
        if session.is_empty() {
            return true;
        }
        self.current
            .lock()
            .get(node)
            .is_none_or(|current| current == session)
    }

    /// Drop a node's lifetime once the node is gone, so the map does not outlive
    /// the cluster it names.
    pub(crate) fn forget(&self, node: &str) {
        self.current.lock().remove(node);
    }

    /// Forget every lifetime. Registrations in another term went to that term's
    /// leader, so nothing recorded here can be vouched for once this one ends.
    pub(crate) fn clear(&self) {
        self.current.lock().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_node_that_has_never_registered_is_vouched_for() {
        // A controller that has just taken over knows no lifetimes, and stalling
        // reconciliation until every node happens to restart strands its capacity.
        let sessions = AgentSessions::default();
        assert!(sessions.vouches_for("n1", "session-a"));
    }

    #[test]
    fn a_cut_from_the_lifetime_that_registered_is_vouched_for() {
        let sessions = AgentSessions::default();
        sessions.observe_registration("n1", "session-a");
        assert!(sessions.vouches_for("n1", "session-a"));
    }

    #[test]
    fn a_cut_from_a_lifetime_a_re_registration_replaced_is_not() {
        let sessions = AgentSessions::default();
        sessions.observe_registration("n1", "session-a");
        sessions.observe_registration("n1", "session-b");
        assert!(!sessions.vouches_for("n1", "session-a"));
        assert!(sessions.vouches_for("n1", "session-b"));
    }

    #[test]
    fn one_nodes_lifetime_says_nothing_about_another() {
        let sessions = AgentSessions::default();
        sessions.observe_registration("n1", "session-a");
        assert!(sessions.vouches_for("n2", "session-a"));
    }

    #[test]
    fn an_agent_that_names_no_lifetime_is_neither_recorded_nor_gated() {
        // Gating a cut that names no lifetime against one it never claimed to be
        // would discard that node's every cut, with no writer able to clear it.
        let sessions = AgentSessions::default();
        sessions.observe_registration("n1", "session-a");
        sessions.observe_registration("n1", "");
        assert!(
            sessions.vouches_for("n1", "session-a"),
            "an empty session must not overwrite a recorded lifetime"
        );
        assert!(sessions.vouches_for("n1", ""));
    }

    #[test]
    fn a_term_that_ends_leaves_no_lifetime_behind() {
        let sessions = AgentSessions::default();
        sessions.observe_registration("n1", "session-a");
        sessions.clear();
        assert!(sessions.vouches_for("n1", "session-b"));
    }

    #[test]
    fn a_removed_node_is_forgotten() {
        let sessions = AgentSessions::default();
        sessions.observe_registration("n1", "session-a");
        sessions.forget("n1");
        assert!(sessions.vouches_for("n1", "session-b"));
    }
}
