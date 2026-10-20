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

/// Generates a crisp vector SVG status badge (flat style)
pub fn render_status_badge(label: &str, record: Option<&JobRecord>) -> String {
    let info = evaluate_status(record);

    let char_width = 7.0;
    let label_len = label.chars().count() as f64;
    let status_len = info.status_text.chars().count() as f64;

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
        status = info.status_text,
        color = info.color_hex,
    )
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

/// Generates a rich 480x140px GitHub Dark-Mode Web Card
pub fn render_card_svg(repo: &str, branch: &str, record: Option<&JobRecord>) -> String {
    let info = evaluate_status(record);

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
        r###"<svg xmlns="http://www.w3.org/2000/svg" width="480" height="140" viewBox="0 0 480 140" fill="none">
  <style>
    .card-bg {{ fill: #0d1117; stroke: #30363d; stroke-width: 1; rx: 8px; }}
    .title {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 14px; font-weight: 600; fill: #58a6ff; }}
    .branch-pill {{ fill: #21262d; stroke: #30363d; stroke-width: 1; rx: 10px; }}
    .branch-text {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 11px; font-weight: 500; fill: #8b949e; }}
    .status-pill {{ fill: {card_badge_bg}; rx: 10px; }}
    .status-text {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 11px; font-weight: 700; fill: #ffffff; }}
    .label {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 11px; font-weight: 400; fill: #8b949e; }}
    .value {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 11px; font-weight: 500; fill: #c9d1d9; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace; }}
    .footer-text {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 10px; fill: #6e7681; }}
  </style>

  <!-- Card Background -->
  <rect width="480" height="140" class="card-bg"/>

  <!-- Header -->
  <text x="20" y="32" class="title">{repo}</text>

  <!-- Branch Pill -->
  <rect x="250" y="18" width="80" height="20" class="branch-pill"/>
  <text x="290" y="32" text-anchor="middle" class="branch-text">{branch}</text>

  <!-- Status Pill -->
  <rect x="340" y="18" width="120" height="20" class="status-pill"/>
  <text x="400" y="32" text-anchor="middle" class="status-text">{icon_char} {status_uppercase}</text>

  <!-- Divider -->
  <line x1="20" y1="48" x2="460" y2="48" stroke="#21262d" stroke-width="1"/>

  <!-- Row 1: Commit & Event -->
  <text x="20" y="70" class="label">Commit:</text>
  <text x="75" y="70" class="value mono">{commit_short}</text>

  <text x="180" y="70" class="label">Trigger:</text>
  <text x="235" y="70" class="value">{trigger_text}</text>

  <!-- Row 2: Duration & Timestamp -->
  <text x="20" y="94" class="label">Duration:</text>
  <text x="75" y="94" class="value mono">{duration_str}</text>

  <text x="180" y="94" class="label">Updated:</text>
  <text x="235" y="94" class="value">{timestamp_str}</text>

  <!-- Footer Banner: Native RISC-V Hardware Tag -->
  <line x1="20" y1="110" x2="460" y2="110" stroke="#21262d" stroke-width="1"/>
  <text x="20" y="127" class="footer-text">SpacemiT Key Stone K3 (8× X100 RISC-V @ 1.6GHz) • ActionRunner-rs Native CI</text>
</svg>"###,
        repo = repo,
        branch = branch,
        card_badge_bg = info.card_badge_bg,
        icon_char = info.icon_char,
        status_uppercase = status_uppercase,
        commit_short = commit_short,
        trigger_text = trigger_text,
        duration_str = duration_str,
        timestamp_str = timestamp_str,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{Job, JobRecord, JobStatus};
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
        };

        let svg = render_status_badge("k3-ci", Some(&record));
        assert!(svg.contains("<svg"));
        assert!(svg.contains("passing"));
        assert!(svg.contains("#2ea44f"));
    }

    #[test]
    fn test_render_status_badge_failing() {
        let record = JobRecord {
            job: sample_job("refs/heads/main"),
            status: JobStatus::Failed {
                error: "Compile error".to_string(),
            },
            log_file: PathBuf::from("logs/sample.log"),
            updated_at: Utc::now(),
        };

        let svg = render_status_badge("k3-ci", Some(&record));
        assert!(svg.contains("failing"));
        assert!(svg.contains("#cb2431"));
    }

    #[test]
    fn test_render_shield_json() {
        let record = JobRecord {
            job: sample_job("refs/heads/main"),
            status: JobStatus::Completed {
                duration_secs: 2,
                exit_code: 0,
            },
            log_file: PathBuf::from("logs/sample.log"),
            updated_at: Utc::now(),
        };

        let json = render_shield_json("k3-ci", Some(&record));
        assert_eq!(json["message"], "passing");
        assert_eq!(json["color"], "brightgreen");
    }

    #[test]
    fn test_render_card_svg() {
        let record = JobRecord {
            job: sample_job("refs/heads/main"),
            status: JobStatus::Completed {
                duration_secs: 2,
                exit_code: 0,
            },
            log_file: PathBuf::from("logs/sample.log"),
            updated_at: Utc::now(),
        };

        let svg = render_card_svg("LMA1980/model-runner", "main", Some(&record));
        assert!(svg.contains("LMA1980/model-runner"));
        assert!(svg.contains("main"));
        assert!(svg.contains("22c90697"));
        assert!(svg.contains("PASSING"));
        assert!(svg.contains("SpacemiT Key Stone K3"));
    }
}
