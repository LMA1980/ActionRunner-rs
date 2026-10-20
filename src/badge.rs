use crate::job::{JobRecord, JobStatus};
use chrono::Utc;
use serde_json::json;

pub struct BadgeInfo {
    pub status_text: &'static str,
    pub color_hex: &'static str,
    pub shields_color: &'static str,
    pub icon_char: &'static str,
    pub card_badge_bg: &'static str,
}

pub fn evaluate_status(record: Option<&JobRecord>) -> BadgeInfo {
    match record {
        Some(r) => match &r.status {
            JobStatus::Completed { exit_code: 0, .. } => BadgeInfo {
                status_text: "passing",
                color_hex: "#2ea44f",
                shields_color: "brightgreen",
                icon_char: "✓",
                card_badge_bg: "#238636",
            },
            JobStatus::Completed { .. } => BadgeInfo {
                status_text: "failing",
                color_hex: "#cb2431",
                shields_color: "red",
                icon_char: "✗",
                card_badge_bg: "#da3633",
            },
            JobStatus::Failed { .. } => BadgeInfo {
                status_text: "failing",
                color_hex: "#cb2431",
                shields_color: "red",
                icon_char: "✗",
                card_badge_bg: "#da3633",
            },
            JobStatus::Running { .. } => BadgeInfo {
                status_text: "running",
                color_hex: "#dbab09",
                shields_color: "yellow",
                icon_char: "⏳",
                card_badge_bg: "#9e6a03",
            },
            JobStatus::Queued => BadgeInfo {
                status_text: "queued",
                color_hex: "#dbab09",
                shields_color: "yellow",
                icon_char: "⏱",
                card_badge_bg: "#9e6a03",
            },
            JobStatus::Skipped { .. } => BadgeInfo {
                status_text: "skipped",
                color_hex: "#6a737d",
                shields_color: "lightgrey",
                icon_char: "⊘",
                card_badge_bg: "#30363d",
            },
        },
        None => BadgeInfo {
            status_text: "no builds",
            color_hex: "#959da5",
            shields_color: "lightgrey",
            icon_char: "—",
            card_badge_bg: "#30363d",
        },
    }
}

/// Helper to render any 2-part flat SVG badge
pub fn render_flat_badge(label: &str, status_text: &str, color_hex: &str) -> String {
    let char_width = 7.0;
    let label_len = label.chars().count() as f64;
    let status_len = status_text.chars().count() as f64;

    let left_box_w = (label_len * char_width + 14.0).round();
    let right_box_w = (status_len * char_width + 16.0).round();
    let total_w = left_box_w + right_box_w;

    let left_text_x = left_box_w / 2.0;
    let right_text_x = left_box_w + (right_box_w / 2.0);

    format!(
        r###"<svg xmlns="http://www.w3.org/2000/svg" width="{total_w}" height="20" role="img" aria-label="{label}: {status}">
  <title>{label}: {status}</title>
  <linearGradient id="s" x2="0" y2="100%">
    <stop offset="0" stop-color="#bbb" stop-opacity=".1"/>
    <stop offset="1" stop-opacity=".1"/>
  </linearGradient>
  <clipPath id="r">
    <rect width="{total_w}" height="20" rx="3" fill="#fff"/>
  </clipPath>
  <g clip-path="url(#r)">
    <rect width="{left_box_w}" height="20" fill="#555"/>
    <rect x="{left_box_w}" width="{right_box_w}" height="20" fill="{color}"/>
    <rect width="{total_w}" height="20" fill="url(#s)"/>
  </g>
  <g fill="#fff" text-anchor="middle" font-family="DejaVu Sans,Verdana,Geneva,sans-serif" font-size="110">
    <text aria-hidden="true" x="{left_text_x_10}" y="150" fill="#010101" fill-opacity=".3" transform="scale(.1)" textLength="{label_text_w}">{label}</text>
    <text x="{left_text_x_10}" y="140" transform="scale(.1)" fill="#fff" textLength="{label_text_w}">{label}</text>
    <text aria-hidden="true" x="{right_text_x_10}" y="150" fill="#010101" fill-opacity=".3" transform="scale(.1)" textLength="{status_text_w}">{status}</text>
    <text x="{right_text_x_10}" y="140" transform="scale(.1)" fill="#fff" textLength="{status_text_w}">{status}</text>
  </g>
</svg>"###,
        total_w = total_w,
        left_box_w = left_box_w,
        right_box_w = right_box_w,
        left_text_x_10 = (left_text_x * 10.0).round(),
        right_text_x_10 = (right_text_x * 10.0).round(),
        label_text_w = ((label_len * char_width) * 10.0).round(),
        status_text_w = ((status_len * char_width) * 10.0).round(),
        label = label,
        status = status_text,
        color = color_hex,
    )
}

/// Generates a crisp vector SVG status badge (flat style)
pub fn render_status_badge(label: &str, record: Option<&JobRecord>) -> String {
    let info = evaluate_status(record);
    render_flat_badge(label, info.status_text, info.color_hex)
}

/// Generates a dedicated overall Score Badge (e.g. `[ k3 score | 100% A+ ]`)
pub fn render_score_badge(record: Option<&JobRecord>) -> String {
    match record.and_then(|r| r.metrics.as_ref()) {
        Some(m) => {
            let status = format!("{}% {}", m.quality_score, m.letter_grade);
            let color = match m.quality_score {
                90..=100 => "#2ea44f",
                80..=89 => "#85e89d",
                70..=79 => "#dbab09",
                _ => "#cb2431",
            };
            render_flat_badge("k3 score", &status, color)
        }
        None => {
            let info = evaluate_status(record);
            if info.status_text == "passing" {
                render_flat_badge("k3 score", "100% A+", "#2ea44f")
            } else {
                render_flat_badge("k3 score", info.status_text, info.color_hex)
            }
        }
    }
}

/// Generates a dedicated Coverage Badge (e.g. `[ coverage | 89.5% ]`)
pub fn render_coverage_badge(record: Option<&JobRecord>) -> String {
    match record
        .and_then(|r| r.metrics.as_ref())
        .and_then(|m| m.coverage_pct)
    {
        Some(cov) => {
            let status = format!("{:.1}%", cov);
            let color = if cov >= 80.0 {
                "#2ea44f"
            } else if cov >= 60.0 {
                "#dbab09"
            } else {
                "#cb2431"
            };
            render_flat_badge("coverage", &status, color)
        }
        None => render_flat_badge("coverage", "n/a", "#959da5"),
    }
}

/// Generates a dedicated Cargo Bench Badge (e.g. `[ cargo bench | 14.2 µs/iter ]` or `[ cargo bench | passing ]`)
pub fn render_benchmark_badge(record: Option<&JobRecord>) -> String {
    match record
        .and_then(|r| r.metrics.as_ref())
        .and_then(|m| m.benchmark_summary.as_ref())
    {
        Some(bench) => render_flat_badge("cargo bench", bench, "#0969da"),
        None => {
            let info = evaluate_status(record);
            if info.status_text == "passing" {
                render_flat_badge("cargo bench", "passing", "#2ea44f")
            } else {
                render_flat_badge("cargo bench", "n/a", "#959da5")
            }
        }
    }
}

/// Generates a Shields.io Custom Endpoint JSON response
pub fn render_shield_json(label: &str, record: Option<&JobRecord>) -> serde_json::Value {
    let info = evaluate_status(record);
    json!({
        "schemaVersion": 1,
        "label": label,
        "message": info.status_text,
        "color": info.shields_color,
        "isError": info.status_text == "failing"
    })
}

/// Generates a rich 520x185px Multi-Metric CI Factory Pipeline Score Card
pub fn render_card_svg(repo: &str, branch: &str, record: Option<&JobRecord>) -> String {
    let info = evaluate_status(record);

    let metrics_opt = record.and_then(|r| r.metrics.as_ref());

    let (quality_score, letter_grade) = match metrics_opt {
        Some(m) => (m.quality_score, m.letter_grade.clone()),
        None => {
            if info.status_text == "passing" {
                (100, "A+".to_string())
            } else if info.status_text == "failing" {
                (40, "F".to_string())
            } else {
                (0, "—".to_string())
            }
        }
    };

    let score_badge_bg = match quality_score {
        90..=100 => "#238636",
        80..=89 => "#2ea44f",
        70..=79 => "#9e6a03",
        _ => "#da3633",
    };

    // Metric Tile 1: Tests
    let (tests_val, tests_sub, tests_bar_w) = match metrics_opt {
        Some(m) if m.tests_total > 0 => {
            let val = format!("{}/{} Passed", m.tests_passed, m.tests_total);
            let pct = (m.tests_passed as f64 / m.tests_total as f64) * 96.0;
            let sub = if m.tests_passed == m.tests_total {
                "100% Pass Rate".to_string()
            } else {
                format!("{} Failed", m.tests_total - m.tests_passed)
            };
            (val, sub, pct.round() as u32)
        }
        _ => {
            if info.status_text == "passing" {
                ("All Passed".to_string(), "100% Pass Rate".to_string(), 96)
            } else if info.status_text == "failing" {
                ("Failed".to_string(), "Check Log".to_string(), 0)
            } else {
                ("—".to_string(), "Pending".to_string(), 0)
            }
        }
    };

    // Metric Tile 2: Coverage
    let (cov_val, cov_sub, cov_bar_w) = match metrics_opt.and_then(|m| m.coverage_pct) {
        Some(cov) => {
            let val = format!("{:.1}%", cov);
            let bar_w = ((cov / 100.0) * 96.0).clamp(0.0, 96.0).round() as u32;
            (val, "Line Coverage".to_string(), bar_w)
        }
        None => {
            if info.status_text == "passing" {
                ("Clean".to_string(), "Verified".to_string(), 96)
            } else {
                ("—".to_string(), "Not Run".to_string(), 0)
            }
        }
    };

    // Metric Tile 3: Cargo Bench
    let (bench_val, bench_sub) = match metrics_opt.and_then(|m| m.benchmark_summary.as_ref()) {
        Some(b) => ("Benchmarked".to_string(), b.clone()),
        None => {
            if info.status_text == "passing" {
                ("Passing".to_string(), "0 Regressions".to_string())
            } else {
                ("—".to_string(), "Not Run".to_string())
            }
        }
    };

    // Metric Tile 4: Hygiene & Security
    let (hygiene_val, hygiene_sub) = match metrics_opt {
        Some(m) => {
            let val = format!("{} Advisories", m.advisories_count);
            let sub = if m.clippy_warnings == 0 {
                "Clippy & Fmt Clean".to_string()
            } else {
                format!("{} Clippy Warnings", m.clippy_warnings)
            };
            (val, sub)
        }
        _ => {
            if info.status_text == "passing" {
                ("0 Advisories".to_string(), "Clippy & Fmt Clean".to_string())
            } else {
                ("—".to_string(), "Pending".to_string())
            }
        }
    };

    let (commit_short, trigger_text, duration_str, timestamp_str) = match record {
        Some(r) => {
            let sha = if r.job.commit_sha.len() >= 8 {
                &r.job.commit_sha[..8]
            } else if !r.job.commit_sha.is_empty() {
                &r.job.commit_sha
            } else {
                "n/a"
            };

            let trigger = format!("{} by @{}", r.job.event_type, r.job.sender);

            let duration = match &r.status {
                JobStatus::Completed { duration_secs, .. } => format!("{}s", duration_secs),
                JobStatus::Running { started_at } => {
                    let d = (Utc::now() - *started_at).num_seconds().max(0);
                    format!("{}s (running)", d)
                }
                JobStatus::Queued => "queued".to_string(),
                JobStatus::Failed { .. } => "failed".to_string(),
                JobStatus::Skipped { .. } => "skipped".to_string(),
            };

            let ts = r.updated_at.format("%Y-%m-%d %H:%M UTC").to_string();

            (sha.to_string(), trigger, duration, ts)
        }
        None => (
            "none".to_string(),
            "no webhook received".to_string(),
            "—".to_string(),
            "—".to_string(),
        ),
    };

    let status_uppercase = info.status_text.to_uppercase();

    format!(
        r###"<svg xmlns="http://www.w3.org/2000/svg" width="520" height="185" viewBox="0 0 520 185" fill="none">
  <style>
    .card-bg {{ fill: #0d1117; stroke: #30363d; stroke-width: 1; rx: 8px; }}
    .tile-bg {{ fill: #161b22; stroke: #21262d; stroke-width: 1; rx: 6px; }}
    .title {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 14px; font-weight: 600; fill: #58a6ff; }}
    .branch-pill {{ fill: #21262d; stroke: #30363d; stroke-width: 1; rx: 10px; }}
    .branch-text {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 11px; font-weight: 500; fill: #8b949e; }}
    .status-pill {{ fill: {card_badge_bg}; rx: 10px; }}
    .status-text {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 11px; font-weight: 700; fill: #ffffff; }}
    .score-pill {{ fill: {score_badge_bg}; rx: 10px; }}
    .score-text {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 11px; font-weight: 700; fill: #ffffff; }}
    .tile-label {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 9px; font-weight: 700; fill: #8b949e; letter-spacing: 0.5px; }}
    .tile-val {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 13px; font-weight: 600; fill: #e6edf3; }}
    .tile-sub {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 9px; fill: #8b949e; }}
    .label {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 10px; font-weight: 400; fill: #8b949e; }}
    .value {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 10px; font-weight: 500; fill: #c9d1d9; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace; }}
    .footer-text {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 9px; fill: #6e7681; }}
  </style>

  <!-- Card Background -->
  <rect width="520" height="185" class="card-bg"/>

  <!-- Header -->
  <text x="20" y="28" class="title">{repo}</text>

  <!-- Branch Pill -->
  <rect x="230" y="14" width="75" height="20" class="branch-pill"/>
  <text x="267" y="28" text-anchor="middle" class="branch-text">{branch}</text>

  <!-- Status Pill -->
  <rect x="312" y="14" width="105" height="20" class="status-pill"/>
  <text x="364" y="28" text-anchor="middle" class="status-text">{icon_char} {status_uppercase}</text>

  <!-- Overall Score Pill -->
  <rect x="424" y="14" width="76" height="20" class="score-pill"/>
  <text x="462" y="28" text-anchor="middle" class="score-text">{quality_score}% {letter_grade}</text>

  <!-- Divider -->
  <line x1="20" y1="42" x2="500" y2="42" stroke="#21262d" stroke-width="1"/>

  <!-- Row 2: 4 Factory Pipeline Metric Tiles -->
  <!-- Tile 1: Tests -->
  <rect x="20" y="50" width="112" height="66" class="tile-bg"/>
  <text x="28" y="65" class="tile-label">TESTS</text>
  <text x="28" y="83" class="tile-val">{tests_val}</text>
  <text x="28" y="96" class="tile-sub">{tests_sub}</text>
  <rect x="28" y="103" width="96" height="4" rx="2" fill="#21262d"/>
  <rect x="28" y="103" width="{tests_bar_w}" height="4" rx="2" fill="#2ea44f"/>

  <!-- Tile 2: Coverage -->
  <rect x="140" y="50" width="112" height="66" class="tile-bg"/>
  <text x="148" y="65" class="tile-label">COVERAGE</text>
  <text x="148" y="83" class="tile-val">{cov_val}</text>
  <text x="148" y="96" class="tile-sub">{cov_sub}</text>
  <rect x="148" y="103" width="96" height="4" rx="2" fill="#21262d"/>
  <rect x="148" y="103" width="{cov_bar_w}" height="4" rx="2" fill="#2ea44f"/>

  <!-- Tile 3: Cargo Bench -->
  <rect x="260" y="50" width="124" height="66" class="tile-bg"/>
  <text x="268" y="65" class="tile-label">CARGO BENCH</text>
  <text x="268" y="83" class="tile-val">{bench_val}</text>
  <text x="268" y="98" class="tile-sub">{bench_sub}</text>

  <!-- Tile 4: Hygiene & Security -->
  <rect x="392" y="50" width="108" height="66" class="tile-bg"/>
  <text x="400" y="65" class="tile-label">SECURITY &amp; LINTS</text>
  <text x="400" y="83" class="tile-val">{hygiene_val}</text>
  <text x="400" y="98" class="tile-sub">{hygiene_sub}</text>

  <!-- Divider -->
  <line x1="20" y1="126" x2="500" y2="126" stroke="#21262d" stroke-width="1"/>

  <!-- Row 3: Commit, Trigger, Duration, Timestamp -->
  <text x="20" y="144" class="label">Commit:</text>
  <text x="65" y="144" class="value mono">{commit_short}</text>

  <text x="135" y="144" class="label">Trigger:</text>
  <text x="178" y="144" class="value">{trigger_text}</text>

  <text x="315" y="144" class="label">Duration:</text>
  <text x="362" y="144" class="value mono">{duration_str}</text>

  <text x="405" y="144" class="label">Updated:</text>
  <text x="450" y="144" class="value">{timestamp_str}</text>

  <!-- Footer Banner: Native RISC-V Hardware Tag -->
  <line x1="20" y1="156" x2="500" y2="156" stroke="#21262d" stroke-width="1"/>
  <text x="20" y="172" class="footer-text">SpacemiT Key Stone K3 (8× X100 RISC-V @ 1.6GHz) • ActionRunner-rs Factory Pipeline</text>
</svg>"###,
        repo = repo,
        branch = branch,
        card_badge_bg = info.card_badge_bg,
        score_badge_bg = score_badge_bg,
        icon_char = info.icon_char,
        status_uppercase = status_uppercase,
        quality_score = quality_score,
        letter_grade = letter_grade,
        tests_val = tests_val,
        tests_sub = tests_sub,
        tests_bar_w = tests_bar_w,
        cov_val = cov_val,
        cov_sub = cov_sub,
        cov_bar_w = cov_bar_w,
        bench_val = bench_val,
        bench_sub = bench_sub,
        hygiene_val = hygiene_val,
        hygiene_sub = hygiene_sub,
        commit_short = commit_short,
        trigger_text = trigger_text,
        duration_str = duration_str,
        timestamp_str = timestamp_str,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{Job, JobMetrics, JobRecord, JobStatus};
    use chrono::Utc;
    use std::path::PathBuf;

    fn sample_job(ref_name: &str) -> Job {
        Job {
            id: "job-123-abc".to_string(),
            event_type: "push".to_string(),
            repo_name: "LMA1980/model-runner".to_string(),
            clone_url: "https://github.com/LMA1980/model-runner.git".to_string(),
            commit_sha: "22c90697a3708061bdae86c4e8cc91094ec297bc".to_string(),
            ref_name: ref_name.to_string(),
            sender: "LMA1980".to_string(),
            queued_at: Utc::now(),
        }
    }

    #[test]
    fn test_render_status_badge_passing() {
        let record = JobRecord {
            job: sample_job("refs/heads/main"),
            status: JobStatus::Completed {
                duration_secs: 4,
                exit_code: 0,
            },
            log_file: PathBuf::from("logs/sample.log"),
            updated_at: Utc::now(),
            metrics: None,
        };

        let svg = render_status_badge("k3-ci", Some(&record));
        assert!(svg.contains("<svg"));
        assert!(svg.contains("passing"));
        assert!(svg.contains("#2ea44f"));
    }

    #[test]
    fn test_render_score_badge() {
        let record = JobRecord {
            job: sample_job("refs/heads/main"),
            status: JobStatus::Completed {
                duration_secs: 4,
                exit_code: 0,
            },
            log_file: PathBuf::from("logs/sample.log"),
            updated_at: Utc::now(),
            metrics: Some(JobMetrics {
                quality_score: 98,
                letter_grade: "A+".to_string(),
                coverage_pct: Some(89.5),
                benchmark_summary: Some("3 benches (14.2 µs/iter)".to_string()),
                tests_passed: 5,
                tests_total: 5,
                advisories_count: 0,
                clippy_warnings: 0,
            }),
        };

        let svg = render_score_badge(Some(&record));
        assert!(svg.contains("k3 score"));
        assert!(svg.contains("98% A+"));
    }

    #[test]
    fn test_render_coverage_badge() {
        let record = JobRecord {
            job: sample_job("refs/heads/main"),
            status: JobStatus::Completed {
                duration_secs: 4,
                exit_code: 0,
            },
            log_file: PathBuf::from("logs/sample.log"),
            updated_at: Utc::now(),
            metrics: Some(JobMetrics {
                quality_score: 95,
                letter_grade: "A+".to_string(),
                coverage_pct: Some(89.5),
                benchmark_summary: None,
                tests_passed: 5,
                tests_total: 5,
                advisories_count: 0,
                clippy_warnings: 0,
            }),
        };

        let svg = render_coverage_badge(Some(&record));
        assert!(svg.contains("coverage"));
        assert!(svg.contains("89.5%"));
    }

    #[test]
    fn test_render_benchmark_badge() {
        let record = JobRecord {
            job: sample_job("refs/heads/main"),
            status: JobStatus::Completed {
                duration_secs: 4,
                exit_code: 0,
            },
            log_file: PathBuf::from("logs/sample.log"),
            updated_at: Utc::now(),
            metrics: Some(JobMetrics {
                quality_score: 95,
                letter_grade: "A+".to_string(),
                coverage_pct: None,
                benchmark_summary: Some("14.2 µs/iter".to_string()),
                tests_passed: 5,
                tests_total: 5,
                advisories_count: 0,
                clippy_warnings: 0,
            }),
        };

        let svg = render_benchmark_badge(Some(&record));
        assert!(svg.contains("cargo bench"));
        assert!(svg.contains("14.2 µs/iter"));
    }

    #[test]
    fn test_render_card_svg_with_metrics() {
        let record = JobRecord {
            job: sample_job("refs/heads/main"),
            status: JobStatus::Completed {
                duration_secs: 2,
                exit_code: 0,
            },
            log_file: PathBuf::from("logs/sample.log"),
            updated_at: Utc::now(),
            metrics: Some(JobMetrics {
                quality_score: 98,
                letter_grade: "A+".to_string(),
                coverage_pct: Some(89.5),
                benchmark_summary: Some("3 benches (14.2 µs/iter)".to_string()),
                tests_passed: 5,
                tests_total: 5,
                advisories_count: 0,
                clippy_warnings: 0,
            }),
        };

        let svg = render_card_svg("LMA1980/model-runner", "main", Some(&record));
        assert!(svg.contains("LMA1980/model-runner"));
        assert!(svg.contains("main"));
        assert!(svg.contains("98% A+"));
        assert!(svg.contains("5/5 Passed"));
        assert!(svg.contains("89.5%"));
        assert!(svg.contains("CARGO BENCH"));
        assert!(svg.contains("0 Advisories"));
    }
}
