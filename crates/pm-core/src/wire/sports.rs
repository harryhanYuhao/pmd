//! Live game-state feed (`wss://sports-api.polymarket.com/ws`).
//!
//! No subscription needed: the server pushes every league. It sends a text `ping` that must be
//! answered with `pong`. `game_id` matches the Gamma event's `gameId`.

use serde::Deserialize;

pub const SPORTS_WS_URL: &str = "wss://sports-api.polymarket.com/ws";

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GameUpdate {
    pub game_id: i64,
    #[serde(default)]
    pub league_abbreviation: String,
    #[serde(default)]
    pub home_team: String,
    #[serde(default)]
    pub away_team: String,
    #[serde(default)]
    pub status: String,
    /// Esports format: `"<in-map score>|<maps won>|Bo<n>"`, e.g. `"23-22|0-1|Bo3"`.
    #[serde(default)]
    pub score: String,
    /// e.g. `"2/3"` = map 2 of a Bo3.
    #[serde(default)]
    pub period: String,
    #[serde(default)]
    pub live: bool,
    #[serde(default)]
    pub ended: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_live_sample() {
        let s = r#"{"gameId":1743416,"leagueAbbreviation":"dota2","homeTeam":"Team Synapse","awayTeam":"Dawn Bulls","status":"running","score":"23-22|0-1|Bo3","period":"2/3","live":true,"ended":false}"#;
        let g: GameUpdate = serde_json::from_str(s).unwrap();
        assert_eq!(g.game_id, 1743416);
        assert!(g.live);
    }
}
