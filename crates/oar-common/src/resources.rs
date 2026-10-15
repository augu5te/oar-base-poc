//! Parsing of the OAR `-l` resource requests.
//!
//! Examples of accepted syntaxes (all equivalent to "2 nodes"):
//!
//! ```text
//! nodes=2,walltime=1:30:00
//! /nodes=2/walltime=1:30:00
//! nodes=2/walltime=1:30
//! core=4/nodes=n1+n2
//! ```
//!
//! As in OAR3 (`oarsub(1)`: "the walltime format is
//! `[hour:mn:sec|hour:mn|hour]`"), the leftmost walltime component is the
//! largest: `2:00` means two hours.
//!
//! The parser is deliberately permissive: unknown `key=value` pairs are kept in
//! [`ResourceRequest::properties`] so that later scheduling placeholders can use
//! them, while the fields the base system understands (`nodes`, `walltime`,
//! cores) are typed.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::types::{JobRequest, ValidationError};

/// A decoded `-l` resource request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRequest {
    pub nodes: u32,
    pub walltime_secs: u32,
    /// Requested cores per node, when given (`core=`, `cores=`, ...).
    pub cores_per_node: Option<u32>,
    /// Every other `key=value` pair, kept verbatim.
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResourceError {
    #[error("empty resource list")]
    Empty,
    #[error("malformed resource item `{0}` (expected key=value)")]
    Malformed(String),
    #[error("invalid number `{value}` for `{key}`")]
    InvalidNumber { key: String, value: String },
    #[error("invalid walltime `{0}`")]
    InvalidWalltime(String),
    #[error("walltime must be greater than zero")]
    ZeroWalltime,
}

impl ResourceRequest {
    pub fn new(nodes: u32, walltime_secs: u32) -> Self {
        Self {
            nodes,
            walltime_secs,
            cores_per_node: None,
            properties: BTreeMap::new(),
        }
    }

    /// Turns the resource request into a submittable [`JobRequest`].
    pub fn to_job_request(
        &self,
        user_uid: u32,
        command: impl Into<String>,
    ) -> Result<JobRequest, ValidationError> {
        let request = JobRequest::new(
            user_uid,
            command,
            self.nodes.max(1),
            self.walltime_secs.max(1),
        );
        request.validate()?;
        Ok(request)
    }
}

/// Parses a `-l` option value (several `-l` options may be joined with `/`).
pub fn parse_resource_list(input: &str) -> Result<ResourceRequest, ResourceError> {
    if input.trim().is_empty() {
        return Err(ResourceError::Empty);
    }

    let mut request = ResourceRequest::default();
    let mut saw_nodes = false;
    let mut saw_walltime = false;

    for raw_item in input.split(['/', ',']) {
        let item = raw_item.trim();
        if item.is_empty() {
            // Tolerate leading/trailing/repeated separators (`-l /nodes=2`).
            continue;
        }
        let Some((raw_key, raw_value)) = item.split_once('=') else {
            return Err(ResourceError::Malformed(item.to_string()));
        };
        let key = raw_key.trim().to_ascii_lowercase();
        let value = raw_value.trim();
        if value.is_empty() {
            return Err(ResourceError::Malformed(item.to_string()));
        }

        match key.as_str() {
            "nodes" | "node" => {
                request.nodes = parse_node_count(value)?;
                saw_nodes = true;
            }
            "walltime" | "walltime_secs" | "walltime_seconds" => {
                request.walltime_secs = parse_walltime(value)?;
                saw_walltime = true;
            }
            "core" | "cores" | "cpu" | "cpus" | "core_per_node" | "cores_per_node" => {
                request.cores_per_node = Some(parse_u32("core", value)?);
            }
            _ => {
                request.properties.insert(key, value.to_string());
            }
        }
    }

    if !saw_nodes {
        request.nodes = 1;
    }
    if !saw_walltime {
        request.walltime_secs = crate::types::DEFAULT_WALLTIME_SECS;
    }
    Ok(request)
}

fn parse_u32(key: &str, value: &str) -> Result<u32, ResourceError> {
    value
        .parse::<u32>()
        .map_err(|_| ResourceError::InvalidNumber {
            key: key.to_string(),
            value: value.to_string(),
        })
}

/// `3`, or a `+`-separated host list (`n1+n2+n3`) whose length is the count.
fn parse_node_count(value: &str) -> Result<u32, ResourceError> {
    if let Ok(count) = value.parse::<u32>() {
        return Ok(count);
    }
    let hosts: Vec<&str> = value
        .split('+')
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .collect();
    if hosts.is_empty() {
        return Err(ResourceError::InvalidNumber {
            key: "nodes".into(),
            value: value.into(),
        });
    }
    Ok(hosts.len() as u32)
}

/// Parses a walltime into seconds.
///
/// Follows OAR3: `[hour:mn:sec|hour:mn|hour]`, i.e. the leftmost component is
/// the largest. A leading `DD-` (or a fourth component) adds days:
/// `1-02:30:00`. Examples: `3` = 3 hours, `2:30` = 2h30, `1:30:00` = 1h30m.
pub fn parse_walltime(input: &str) -> Result<u32, ResourceError> {
    let value = input.trim();
    if value.is_empty() {
        return Err(ResourceError::InvalidWalltime(input.to_string()));
    }

    // `1-02:30:00` (days use `-`).
    if let Some((days, rest)) = value.split_once('-') {
        let days = parse_component(days, input)?;
        let seconds = parse_clock(rest, input)?;
        return finish(u64::from(days) * 86400 + seconds, input);
    }

    finish(parse_clock(value, input)?, input)
}

fn parse_clock(value: &str, original: &str) -> Result<u64, ResourceError> {
    let parts: Vec<&str> = value.split(':').collect();
    if parts.is_empty() || parts.len() > 4 {
        return Err(ResourceError::InvalidWalltime(original.to_string()));
    }
    let numbers: Vec<u64> = parts
        .iter()
        .map(|p| parse_component(p, original).map(u64::from))
        .collect::<Result<_, _>>()?;

    // Leftmost component is the largest; `checked_*` refuses absurd values.
    let invalid = || ResourceError::InvalidWalltime(original.to_string());
    let seconds = match numbers.as_slice() {
        [h] => h.checked_mul(3600),
        [h, m] => h
            .checked_mul(3600)
            .and_then(|v| v.checked_add(m.checked_mul(60)?)),
        [h, m, s] => h
            .checked_mul(3600)
            .and_then(|v| v.checked_add(m.checked_mul(60)?))
            .and_then(|v| v.checked_add(*s)),
        [d, h, m, s] => d
            .checked_mul(86400)
            .and_then(|v| v.checked_add(h.checked_mul(3600)?))
            .and_then(|v| v.checked_add(m.checked_mul(60)?))
            .and_then(|v| v.checked_add(*s)),
        _ => None,
    }
    .ok_or_else(invalid)?;
    Ok(seconds)
}

fn parse_component(value: &str, original: &str) -> Result<u32, ResourceError> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| ResourceError::InvalidWalltime(original.to_string()))
}

/// `u32` cannot hold more than ~136 years of seconds: refuse overflow loudly.
fn finish(total: u64, original: &str) -> Result<u32, ResourceError> {
    if total == 0 {
        return Err(ResourceError::ZeroWalltime);
    }
    u32::try_from(total).map_err(|_| ResourceError::InvalidWalltime(original.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_nodes() {
        let r = parse_resource_list("nodes=4").unwrap();
        assert_eq!(r.nodes, 4);
        assert_eq!(r.walltime_secs, crate::types::DEFAULT_WALLTIME_SECS);
        assert!(r.properties.is_empty());
    }

    #[test]
    fn parses_oar3_style_leading_slash() {
        let r = parse_resource_list("/resource_id=1").unwrap();
        assert_eq!(r.nodes, 1);
        assert_eq!(
            r.properties.get("resource_id").map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn parses_nodes_and_walltime() {
        let r = parse_resource_list("nodes=2,walltime=1:30:00").unwrap();
        assert_eq!(r.nodes, 2);
        assert_eq!(r.walltime_secs, 5400);
    }

    #[test]
    fn parses_slash_separated_and_cores() {
        let r = parse_resource_list("core=4/nodes=n1+n2+n3/walltime=10:00").unwrap();
        assert_eq!(r.nodes, 3);
        assert_eq!(r.cores_per_node, Some(4));
        assert_eq!(r.walltime_secs, 10 * 3600);
    }

    #[test]
    fn unknown_properties_are_preserved() {
        let r = parse_resource_list("nodes=1/type=default/gpu=2").unwrap();
        assert_eq!(
            r.properties.get("type").map(String::as_str),
            Some("default")
        );
        assert_eq!(r.properties.get("gpu").map(String::as_str), Some("2"));
    }

    #[test]
    fn malformed_items_are_refused() {
        assert_eq!(parse_resource_list("").unwrap_err(), ResourceError::Empty);
        assert!(matches!(
            parse_resource_list("nodes"),
            Err(ResourceError::Malformed(_))
        ));
        assert!(matches!(
            parse_resource_list("nodes="),
            Err(ResourceError::Malformed(_))
        ));
        assert!(matches!(
            parse_resource_list("nodes=+"),
            Err(ResourceError::InvalidNumber { .. })
        ));
    }

    #[test]
    fn a_single_hostname_counts_as_one_node() {
        assert_eq!(parse_resource_list("nodes=host1").unwrap().nodes, 1);
    }

    #[test]
    fn walltime_formats_follow_oar3() {
        // Leftmost component is the largest: `3` is three hours.
        assert_eq!(parse_walltime("3").unwrap(), 3 * 3600);
        assert_eq!(parse_walltime("10:00").unwrap(), 10 * 3600);
        assert_eq!(parse_walltime("2:30").unwrap(), 2 * 3600 + 30 * 60);
        assert_eq!(parse_walltime("1:30:00").unwrap(), 5400);
        assert_eq!(parse_walltime("1:00:00:00").unwrap(), 86_400);
        assert_eq!(parse_walltime("1-00:00:00").unwrap(), 86_400);
        assert_eq!(parse_walltime("2-01:00:00").unwrap(), 2 * 86_400 + 3600);
        assert_eq!(parse_walltime(" 4 ").unwrap(), 4 * 3600);
    }

    #[test]
    fn walltime_errors() {
        assert!(matches!(
            parse_walltime(""),
            Err(ResourceError::InvalidWalltime(_))
        ));
        assert!(matches!(
            parse_walltime("aa:bb"),
            Err(ResourceError::InvalidWalltime(_))
        ));
        assert!(matches!(
            parse_walltime("1:2:3:4:5"),
            Err(ResourceError::InvalidWalltime(_))
        ));
        assert!(matches!(
            parse_walltime("0"),
            Err(ResourceError::ZeroWalltime)
        ));
        assert!(matches!(
            parse_walltime("0:00"),
            Err(ResourceError::ZeroWalltime)
        ));
        assert!(matches!(
            parse_walltime("0:00:00:00"),
            Err(ResourceError::ZeroWalltime)
        ));
    }

    #[test]
    fn to_job_request_validates() {
        let r = parse_resource_list("nodes=2,walltime=1:00").unwrap();
        let request = r.to_job_request(1000, "echo hi").unwrap();
        assert_eq!(request.nodes, 2);
        assert_eq!(request.walltime_secs, 3600);
        assert_eq!(request.user_uid, 1000);
        assert!(r.to_job_request(1000, "   ").is_err());
    }
}
