// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};

use super::db::UsageRecord;

/// Compute per-(user, account) fair-share factors using hierarchical BFS shares,
/// per-user association weights, and decay-weighted CPU+GPU billable usage.
pub(super) fn compute_fairshare(
    usage: &[UsageRecord],
    accounts: &[super::db::AccountRecord],
    user_weights: &HashMap<(String, String), i32>,
    halflife_days: u32,
    now: DateTime<Utc>,
) -> HashMap<(String, String), f64> {
    if accounts.is_empty() {
        return HashMap::new();
    }

    let effective_shares = build_effective_shares(accounts);
    if effective_shares.is_empty() {
        return HashMap::new();
    }

    let halflife = Duration::days(halflife_days as i64);
    let decay_rate = 2.0_f64.ln() / halflife.num_seconds() as f64;

    let mut user_usage: HashMap<(String, String), f64> = HashMap::new();
    for record in usage {
        let age = (now - record.period_start).num_seconds().max(0) as f64;
        let decay = (-decay_rate * age).exp();
        let billable = (record.cpu_seconds + record.gpu_seconds) as f64 * decay;

        *user_usage
            .entry((record.user_name.clone(), record.account.clone()))
            .or_insert(0.0) += billable;
    }

    let total_usage: f64 = user_usage.values().sum();
    let epsilon = 0.001;

    let mut all_pairs: std::collections::HashSet<(String, String)> = user_usage.keys().cloned().collect();
    for key in user_weights.keys() {
        all_pairs.insert(key.clone());
    }

    let mut users_per_account: HashMap<String, Vec<String>> = HashMap::new();
    for (user, account) in &all_pairs {
        users_per_account
            .entry(account.clone())
            .or_default()
            .push(user.clone());
    }

    let mut user_shares: HashMap<(String, String), f64> = HashMap::new();
    for (account, users) in &users_per_account {
        let account_share = effective_shares.get(account).copied().unwrap_or(0.0);
        if account_share <= 0.0 {
            for user in users {
                user_shares.insert((user.clone(), account.clone()), 0.0);
            }
            continue;
        }

        let weight_sum: i32 = users
            .iter()
            .map(|u| {
                user_weights
                    .get(&(u.clone(), account.clone()))
                    .copied()
                    .unwrap_or(1)
            })
            .sum();
        let weight_sum = weight_sum.max(1) as f64;

        for user in users {
            let w = user_weights
                .get(&(user.clone(), account.clone()))
                .copied()
                .unwrap_or(1) as f64;
            let share = (w / weight_sum) * account_share;
            user_shares.insert((user.clone(), account.clone()), share);
        }
    }

    let mut factors = HashMap::new();
    for (key, user_share) in &user_shares {
        let usage_val = user_usage.get(key).copied().unwrap_or(0.0);
        let factor = if usage_val < epsilon {
            (*user_share / epsilon).min(100.0)
        } else {
            let actual_share = usage_val / total_usage.max(epsilon);
            (user_share / actual_share).min(100.0)
        };
        factors.insert(key.clone(), factor);
    }

    factors
}

/// BFS from root accounts, splitting parent share proportionally by sibling weight.
/// Accounts whose parent is missing from the list are silently excluded.
fn build_effective_shares(accounts: &[super::db::AccountRecord]) -> HashMap<String, f64> {
    let weight_map: HashMap<&str, i32> = accounts
        .iter()
        .map(|a| (a.name.as_str(), a.fairshare_weight))
        .collect();

    let mut children_of: HashMap<Option<&str>, Vec<&str>> = HashMap::new();
    for a in accounts {
        children_of
            .entry(a.parent.as_deref())
            .or_default()
            .push(&a.name);
    }

    let mut effective: HashMap<String, f64> = HashMap::new();

    let mut queue: std::collections::VecDeque<(Option<&str>, f64)> = std::collections::VecDeque::new();
    queue.push_back((None, 1.0));

    while let Some((parent, parent_share)) = queue.pop_front() {
        let Some(kids) = children_of.get(&parent) else {
            continue;
        };
        let sibling_weight_sum: i32 = kids
            .iter()
            .map(|name| weight_map.get(name).copied().unwrap_or(1))
            .sum();
        let sibling_weight_sum = sibling_weight_sum.max(1) as f64;

        for name in kids {
            let w = weight_map.get(name).copied().unwrap_or(1) as f64;
            let share = (w / sibling_weight_sum) * parent_share;
            effective.insert(name.to_string(), share);
            queue.push_back((Some(name), share));
        }
    }

    effective
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_account(name: &str, parent: Option<&str>, weight: i32) -> super::super::db::AccountRecord {
        super::super::db::AccountRecord {
            name: name.into(),
            description: String::new(),
            organization: String::new(),
            parent: parent.map(String::from),
            fairshare_weight: weight,
            max_running_jobs: None,
            grp_tres: None,
        }
    }

    #[test]
    fn test_compute_fairshare() {
        let now = Utc::now();
        let usage = vec![
            UsageRecord {
                user_name: "alice".into(),
                account: "research".into(),
                cpu_seconds: 100_000,
                gpu_seconds: 50_000,
                job_count: 10,
                period_start: now - Duration::days(1),
            },
            UsageRecord {
                user_name: "bob".into(),
                account: "engineering".into(),
                cpu_seconds: 10_000,
                gpu_seconds: 0,
                job_count: 2,
                period_start: now - Duration::days(1),
            },
        ];

        let accounts = vec![
            make_account("research", None, 1),
            make_account("engineering", None, 1),
        ];

        let mut user_weights = HashMap::new();
        user_weights.insert(("alice".into(), "research".into()), 1);
        user_weights.insert(("bob".into(), "engineering".into()), 1);

        let factors = compute_fairshare(&usage, &accounts, &user_weights, 14, now);

        let alice_factor = factors.get(&("alice".into(), "research".into())).unwrap();
        let bob_factor = factors.get(&("bob".into(), "engineering".into())).unwrap();
        assert!(*bob_factor > 1.0);
        assert!(*alice_factor < 1.0);
        assert!(bob_factor > alice_factor);
    }

    #[test]
    fn test_gpu_seconds_included_in_billable() {
        let now = Utc::now();
        let usage = vec![
            UsageRecord {
                user_name: "alice".into(),
                account: "research".into(),
                cpu_seconds: 10_000,
                gpu_seconds: 90_000,
                job_count: 5,
                period_start: now - Duration::days(1),
            },
            UsageRecord {
                user_name: "bob".into(),
                account: "research".into(),
                cpu_seconds: 10_000,
                gpu_seconds: 0,
                job_count: 5,
                period_start: now - Duration::days(1),
            },
        ];

        let accounts = vec![make_account("research", None, 1)];
        let mut user_weights = HashMap::new();
        user_weights.insert(("alice".into(), "research".into()), 1);
        user_weights.insert(("bob".into(), "research".into()), 1);

        let factors = compute_fairshare(&usage, &accounts, &user_weights, 14, now);
        let alice = factors.get(&("alice".into(), "research".into())).unwrap();
        let bob = factors.get(&("bob".into(), "research".into())).unwrap();
        assert!(bob > alice);
    }

    #[test]
    fn test_account_hierarchy() {
        let now = Utc::now();
        let accounts = vec![
            make_account("root", None, 1),
            make_account("child_a", Some("root"), 3),
            make_account("child_b", Some("root"), 1),
        ];

        let usage = vec![
            UsageRecord {
                user_name: "alice".into(),
                account: "child_a".into(),
                cpu_seconds: 10_000,
                gpu_seconds: 0,
                job_count: 1,
                period_start: now - Duration::days(1),
            },
            UsageRecord {
                user_name: "bob".into(),
                account: "child_b".into(),
                cpu_seconds: 10_000,
                gpu_seconds: 0,
                job_count: 1,
                period_start: now - Duration::days(1),
            },
        ];

        let mut user_weights = HashMap::new();
        user_weights.insert(("alice".into(), "child_a".into()), 1);
        user_weights.insert(("bob".into(), "child_b".into()), 1);

        let factors = compute_fairshare(&usage, &accounts, &user_weights, 14, now);
        let alice = factors.get(&("alice".into(), "child_a".into())).unwrap();
        let bob = factors.get(&("bob".into(), "child_b".into())).unwrap();
        assert!(alice > bob);
    }

    #[test]
    fn test_zero_usage_boost() {
        let now = Utc::now();
        let accounts = vec![make_account("research", None, 1)];
        let user_weights: HashMap<(String, String), i32> = vec![
            (("idle_user".into(), "research".into()), 1),
        ]
        .into_iter()
        .collect();

        let factors = compute_fairshare(&[], &accounts, &user_weights, 14, now);
        let idle = factors.get(&("idle_user".into(), "research".into())).unwrap();
        assert!(*idle > 1.0);
    }
}
