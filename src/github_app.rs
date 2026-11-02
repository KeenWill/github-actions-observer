//! Short-lived, read-only GitHub App installation authentication.
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::BTreeMap, path::Path, time::Duration};

const CLOCK_SKEW_SECONDS: i64 = 60;
const JWT_LIFETIME_SECONDS: i64 = 480;
const MINIMUM_TOKEN_LIFETIME_SECONDS: i64 = 60;

#[derive(Serialize)]
struct Claims<'a> {
    iat: i64,
    exp: i64,
    iss: &'a str,
}

#[derive(Deserialize)]
struct InstallationToken {
    token: String,
    expires_at: DateTime<Utc>,
    permissions: BTreeMap<String, String>,
}

/// Reject URL components that could leak credentials or alter authenticated endpoints.
pub fn validate_api_base(api_base: &Url) -> Result<()> {
    ensure!(
        api_base.scheme() == "https",
        "GitHub API base must use HTTPS"
    );
    ensure!(
        api_base.host_str().is_some()
            && api_base.username().is_empty()
            && api_base.password().is_none()
            && api_base.query().is_none()
            && api_base.fragment().is_none()
            && api_base.path().ends_with('/'),
        "GitHub API base must end in / and contain no credentials, query or fragment"
    );
    Ok(())
}

fn signed_jwt(issuer: &str, private_key: &[u8], now: i64) -> Result<String> {
    ensure!(
        !issuer.trim().is_empty(),
        "GitHub App issuer must not be empty"
    );
    let key = EncodingKey::from_rsa_pem(private_key)
        .map_err(|_| anyhow::anyhow!("GitHub App key must be an RSA private key in PEM format"))?;
    encode(
        &Header::new(Algorithm::RS256),
        &Claims {
            iat: now - CLOCK_SKEW_SECONDS,
            exp: now + JWT_LIFETIME_SECONDS,
            iss: issuer,
        },
        &key,
    )
    .map_err(|_| anyhow::anyhow!("could not sign GitHub App JWT"))
}

async fn exchange(api_base: &Url, installation_id: u64, jwt: &str) -> Result<String> {
    ensure!(
        installation_id > 0,
        "GitHub App installation ID must be positive"
    );
    let client = Client::builder()
        .user_agent("github-actions-observer")
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    let response = client
        .post(api_base.join(&format!(
            "app/installations/{installation_id}/access_tokens"
        ))?)
        .bearer_auth(jwt)
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .json(&json!({"permissions":{"actions":"read","metadata":"read"}}))
        .send()
        .await
        .context("GitHub App token request failed; no retry attempted")?;
    ensure!(
        response.status().is_success(),
        "GitHub App token request returned {}; no retry attempted",
        response.status()
    );
    let token: InstallationToken = response
        .json()
        .await
        .map_err(|_| anyhow::anyhow!("invalid GitHub installation token response"))?;
    ensure!(
        !token.token.is_empty(),
        "GitHub returned an empty installation token"
    );
    ensure!(
        token.expires_at > Utc::now() + chrono::Duration::seconds(MINIMUM_TOKEN_LIFETIME_SECONDS),
        "GitHub returned an expired or nearly expired installation token"
    );
    ensure!(
        token
            .permissions
            .get("actions")
            .is_some_and(|access| access == "read")
            && token.permissions.iter().all(|(name, access)| matches!(
                name.as_str(),
                "actions" | "metadata"
            ) && access == "read"),
        "GitHub installation token did not have the requested read-only permissions"
    );
    tracing::info!(expires_at=%token.expires_at, "read-only GitHub App installation token obtained");
    Ok(token.token)
}

/// Read a mounted key and mint a fresh token for one bounded reconciliation invocation.
pub async fn installation_token(
    api_base: &Url,
    issuer: &str,
    installation_id: u64,
    private_key_file: &Path,
) -> Result<String> {
    validate_api_base(api_base)?;
    let private_key = tokio::fs::read(private_key_file)
        .await
        .context("could not read GitHub App private key file")?;
    let jwt = signed_jwt(issuer, &private_key, Utc::now().timestamp())?;
    exchange(api_base, installation_id, &jwt).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::State,
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::post,
    };
    use jsonwebtoken::{DecodingKey, Validation, decode};

    const KEY: &[u8] = include_bytes!("../tests/fixtures/app-test-key.pem");
    const PUBLIC_KEY: &[u8] = include_bytes!("../tests/fixtures/app-test-public.pem");

    #[derive(Debug, Deserialize)]
    struct VerifiedClaims {
        iat: i64,
        exp: i64,
        iss: String,
    }

    #[test]
    fn signed_jwt_uses_rs256_issuer_and_bounded_clock_skew() -> Result<()> {
        let now = Utc::now().timestamp();
        for key in [
            KEY.to_vec(),
            String::from_utf8(KEY.to_vec())?
                .replace('\n', " ")
                .into_bytes(),
        ] {
            let jwt = signed_jwt("test-app", &key, now)?;
            let decoded = decode::<VerifiedClaims>(
                &jwt,
                &DecodingKey::from_rsa_pem(PUBLIC_KEY)?,
                &Validation::new(Algorithm::RS256),
            )?;
            assert_eq!(decoded.header.alg, Algorithm::RS256);
            assert_eq!(decoded.claims.iss, "test-app");
            assert_eq!(decoded.claims.iat, now - 60);
            assert_eq!(decoded.claims.exp, now + 480);
        }
        Ok(())
    }

    #[test]
    fn credentials_and_invalid_keys_are_not_exposed() {
        for url in [
            "http://api.example/",
            "https://user:secret@api.example/",
            "https://api.example/?secret=x",
            "https://api.example/#secret",
            "https://api.example/api/v3",
        ] {
            assert!(validate_api_base(&Url::parse(url).unwrap()).is_err());
        }
        assert!(validate_api_base(&Url::parse("https://api.example/api/v3/").unwrap()).is_ok());
        let error = signed_jwt(
            "test",
            b"PRIVATE_SECRET_INVALID_KEY",
            Utc::now().timestamp(),
        )
        .unwrap_err();
        assert!(!format!("{error:#}").contains("PRIVATE_SECRET"));
    }

    async fn token_response(
        State(mode): State<&'static str>,
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> Response {
        let jwt = headers["authorization"]
            .to_str()
            .unwrap()
            .strip_prefix("Bearer ")
            .unwrap();
        let decoded = decode::<VerifiedClaims>(
            jwt,
            &DecodingKey::from_rsa_pem(PUBLIC_KEY).unwrap(),
            &Validation::new(Algorithm::RS256),
        )
        .unwrap();
        assert_eq!(decoded.claims.iss, "test-app");
        assert_eq!(
            body,
            json!({"permissions":{"actions":"read","metadata":"read"}})
        );
        if mode == "denied" {
            return (StatusCode::UNAUTHORIZED, "PRIVATE_SECRET_RESPONSE").into_response();
        }
        if mode == "redirect" {
            return (
                StatusCode::TEMPORARY_REDIRECT,
                [("location", "/unexpected")],
            )
                .into_response();
        }
        Json(json!({"token":"test-installation-token", "expires_at": Utc::now()+chrono::Duration::seconds(if mode=="expired" {-1}else{3600}), "permissions":{"actions":if mode=="write" {"write"}else{"read"}, "metadata":"read"}})).into_response()
    }

    #[tokio::test]
    async fn exchange_limits_permissions_and_rejects_redirects_expiry_and_errors() -> Result<()> {
        for mode in ["ok", "denied", "redirect", "expired", "write"] {
            let routes = Router::new()
                .route("/app/installations/42/access_tokens", post(token_response))
                .with_state(mode);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let url = Url::parse(&format!("http://{}/", listener.local_addr()?))?;
            let server = tokio::spawn(async move { axum::serve(listener, routes).await });
            let jwt = signed_jwt("test-app", KEY, Utc::now().timestamp())?;
            let result = exchange(&url, 42, &jwt).await;
            if mode == "ok" {
                assert_eq!(result?, "test-installation-token");
            } else {
                let error = format!("{:#}", result.unwrap_err());
                assert!(!error.contains("PRIVATE_SECRET"));
                assert!(!error.contains(&jwt));
            }
            server.abort();
        }
        Ok(())
    }
}
