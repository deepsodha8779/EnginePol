use domain::envelope::TadpoleHead;
use log::{debug, warn};

#[derive(Debug)]
pub struct ExprError {
    pub message: String,
}

fn parse_eq_term(term: &str) -> Result<(&str, &str), ExprError> {
    let parts: Vec<&str> = term.split("==").map(|s| s.trim()).collect();
    if parts.len() != 2 {
        warn!("expr parse failed: unsupported term={}", term);
        return Err(ExprError {
            message: format!("unsupported expression term: {term}"),
        });
    }

    let ident = parts[0];
    let raw_value = parts[1];
    let value = raw_value
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .ok_or_else(|| ExprError {
            message: format!("expected quoted string literal in: {term}"),
        })?;

    debug!("expr parsed term: ident={} value={}", ident, value);
    Ok((ident, value))
}

pub fn eval_head_expr(expr: &str, head: &TadpoleHead) -> Result<bool, ExprError> {
    debug!(
        "expr eval start: expr={} event_id={} tenant_id={} event_name={}",
        expr, head.event_id, head.tenant_id, head.event_name
    );
    // Minimal CEL-like subset for MVP:
    // - conjunction with &&
    // - equality terms: `event_name == "..."`, `tenant_id == "..."`
    let terms = expr.split("&&").map(|s| s.trim()).filter(|s| !s.is_empty());
    for term in terms {
        let (ident, expected) = parse_eq_term(term)?;
        let actual = match ident {
            "event_name" => head.event_name.as_str(),
            "event_category" => head.event_category.as_deref().unwrap_or(""),
            "tenant_id" => head.tenant_id.as_str(),
            "event_id" => head.event_id.as_str(),
            _ => {
                warn!("expr eval unknown identifier: {}", ident);
                return Err(ExprError {
                    message: format!("unknown identifier: {ident}"),
                });
            }
        };
        if actual != expected {
            debug!(
                "expr eval mismatch: ident={} expected={} actual={}",
                ident, expected, actual
            );
            return Ok(false);
        }
    }

    debug!("expr eval match: expr={}", expr);
    Ok(true)
}
