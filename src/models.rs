use serde::Serialize;

#[derive(Serialize)]
pub struct Row<'a> {
    pub date: &'a str,
    pub revenue: &'a str,
    pub gdp_share: &'a str,
}