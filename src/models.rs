use serde::Serialize;

#[derive(Serialize)]
pub struct Row<'a> {
    pub date: &'a str,
    pub revenue: &'a str,
    pub small_letters_share: &'a str,
    pub long_range_share: &'a str,
    pub gdp_share: f32,
}
