-- 待機プレイヤー用テーブル
CREATE TABLE IF NOT EXISTS matchmaking_queue (
    player_id VARCHAR(64) PRIMARY KEY,
    rating INT,
    waiting_since DATETIME
);

-- セッション情報用テーブル
CREATE TABLE IF NOT EXISTS sessions (
    session_id VARCHAR(64) PRIMARY KEY,
    player1_id VARCHAR(64),
    player2_id VARCHAR(64),
    start_time DATETIME
);

