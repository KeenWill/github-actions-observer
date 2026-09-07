use anyhow::{Result, ensure};
use serde_json::Value;

/// An empty include list (or `*`) accepts future repositories and event types.
#[derive(Clone, Default)]
pub struct Filters {
    repositories_include: Vec<String>,
    repositories_exclude: Vec<String>,
    events_include: Vec<String>,
    events_exclude: Vec<String>,
}

impl Filters {
    pub fn new(
        repositories_include: &str,
        repositories_exclude: &str,
        events_include: &str,
        events_exclude: &str,
    ) -> Result<Self> {
        fn parse(value: &str) -> Vec<String> {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_ascii_lowercase)
                .collect()
        }
        let filters = Self {
            repositories_include: parse(repositories_include),
            repositories_exclude: parse(repositories_exclude),
            events_include: parse(events_include),
            events_exclude: parse(events_exclude),
        };
        for repository in filters
            .repositories_include
            .iter()
            .chain(&filters.repositories_exclude)
        {
            ensure!(
                repository == "*" || valid_repository(repository),
                "repository filters must be owner/repository or *"
            );
        }
        Ok(filters)
    }

    pub fn accepts(&self, event: &str, payload: &Value) -> bool {
        let repository = payload
            .pointer("/repository/full_name")
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase);
        let event = event.to_ascii_lowercase();
        let action = payload
            .get("action")
            .and_then(Value::as_str)
            .map(|action| format!("{event}.{}", action.to_ascii_lowercase()));
        fn matches(patterns: &[String], values: &[Option<&str>]) -> bool {
            patterns.iter().any(|pattern| {
                pattern == "*" || values.iter().flatten().any(|value| pattern == value)
            })
        }
        let repository_values = [repository.as_deref()];
        let event_values = [Some(event.as_str()), action.as_deref()];
        (self.repositories_include.is_empty()
            || matches(&self.repositories_include, &repository_values))
            && !matches(&self.repositories_exclude, &repository_values)
            && (self.events_include.is_empty() || matches(&self.events_include, &event_values))
            && !matches(&self.events_exclude, &event_values)
    }
}

pub fn valid_repository(repository: &str) -> bool {
    let parts: Vec<_> = repository.split('/').collect();
    parts.len() == 2
        && parts.iter().all(|part| {
            !part.is_empty()
                && *part != "."
                && *part != ".."
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn defaults_accept_future_repositories_unknown_events_and_nonrepository_events() {
        let filters = Filters::default();
        assert!(filters.accepts(
            "future_event",
            &json!({"repository":{"full_name":"example-org/new-repo"}})
        ));
        assert!(filters.accepts("ping", &json!({})));
    }
    #[test]
    fn exclusions_win_and_repository_names_are_case_insensitive() {
        let filters = Filters::new(
            "Example-Org/Example-Repo",
            "",
            "workflow_job",
            "workflow_job.queued",
        )
        .unwrap();
        assert!(filters.accepts(
            "workflow_job",
            &json!({"action":"completed","repository":{"full_name":"example-org/example-repo"}})
        ));
        assert!(!filters.accepts(
            "workflow_job",
            &json!({"action":"queued","repository":{"full_name":"example-org/example-repo"}})
        ));
        assert!(!filters.accepts("workflow_job", &json!({})));
        assert!(
            !Filters::new("*", "*", "*", "")
                .unwrap()
                .accepts("ping", &json!({}))
        );
    }
}
