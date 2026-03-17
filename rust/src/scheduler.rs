use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct SmartSchedule {
    pub per_worker: Vec<Vec<String>>,
    pub known_count: usize,
    pub unknown_count: usize,
    pub estimated_totals_s: Vec<f64>,
}

pub fn smart_schedule_units(
    units: &[Vec<String>],
    num_workers: usize,
    avg_durations_s: &HashMap<String, f64>,
) -> SmartSchedule {
    let n = num_workers.max(1);
    let mut known_units: Vec<(usize, f64, usize, usize)> = Vec::new(); // (idx, est, known_cnt, unknown_cnt)
    let mut unknown_units: Vec<(usize, usize)> = Vec::new(); // (idx, unknown_cnt)

    let mut total_known = 0usize;
    let mut total_unknown = 0usize;

    for (idx, unit) in units.iter().enumerate() {
        let mut est = 0.0;
        let mut unit_any_known = false;
        let mut known_cnt = 0usize;
        let mut unknown_cnt = 0usize;
        for nid in unit {
            if let Some(d) = avg_durations_s.get(nid) {
                unit_any_known = true;
                known_cnt += 1;
                total_known += 1;
                est += d.max(0.0);
            } else {
                unknown_cnt += 1;
                total_unknown += 1;
            }
        }
        if unit_any_known {
            known_units.push((idx, est, known_cnt, unknown_cnt));
        } else {
            unknown_units.push((idx, unknown_cnt));
        }
    }

    known_units.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut per_worker: Vec<Vec<String>> = vec![Vec::new(); n];
    let mut totals: Vec<f64> = vec![0.0; n];

    for (unit_idx, est, _k, _u) in known_units {
        let widx = (0..n)
            .min_by(|&i, &j| totals[i].partial_cmp(&totals[j]).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or(0);
        per_worker[widx].extend(units[unit_idx].iter().cloned());
        totals[widx] += est;
    }

    for (i, (unit_idx, _u)) in unknown_units.into_iter().enumerate() {
        per_worker[i % n].extend(units[unit_idx].iter().cloned());
    }

    SmartSchedule {
        per_worker,
        known_count: total_known,
        unknown_count: total_unknown,
        estimated_totals_s: totals,
    }
}

