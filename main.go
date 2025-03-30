package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Player はユーザ情報を表します。
type Player struct {
	ID     string `json:"id"`
	Rating int    `json:"rating"`
}

// SessionResult は対戦セッションの結果を表します。
type SessionResult struct {
	SessionID string `json:"session_id"`
	Player1   Player `json:"player1"`
	Player2   Player `json:"player2"`
}

var (
	// グローバルDB接続
	db *sql.DB

	// 待機中のプレイヤーと対応するマッチ結果を返すチャネルのマップ
	waitingChans      = make(map[string]chan SessionResult)
	waitingChansMutex sync.Mutex
)

// initDB は MySQL への接続を初期化します。
// DSN は適宜ご自身の環境に合わせて変更してください。
func initDB() error {
	// 例: "user:password@tcp(127.0.0.1:3306)/dbname?parseTime=true&multiStatements=true"
	dsn := "yusuke:password@tcp(127.0.0.1:3306)/matchmaking?parseTime=true"
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("DB接続エラー: %v", err)
	}
	if err = db.Ping(); err != nil {
		return fmt.Errorf("DB Pingエラー: %v", err)
	}
	return nil
}

// initSchemaFromFile は、外部ファイルからスキーマ情報を読み込みテーブルを作成します。
func initSchemaFromFile(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("スキーマファイル読み込みエラー: %v", err)
	}

	// ファイル内のSQL文をセミコロンで分割して個別に実行する
	stmts := strings.Split(string(data), ";")
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		_, err := db.Exec(stmt)
		if err != nil {
			return fmt.Errorf("ステートメント実行エラー [%s]: %v", stmt, err)
		}
	}
	return nil
}

// insertWaitingPlayer は待機プレイヤーを DB に登録します。
func insertWaitingPlayer(p Player) error {
	query := "INSERT INTO matchmaking_queue (player_id, rating, waiting_since) VALUES (?, ?, NOW())"
	_, err := db.Exec(query, p.ID, p.Rating)
	return err
}

// deleteWaitingPlayer は指定プレイヤーを DB の待機キューから削除します。
func deleteWaitingPlayer(playerID string) error {
	query := "DELETE FROM matchmaking_queue WHERE player_id = ?"
	_, err := db.Exec(query, playerID)
	return err
}

// getWaitingPlayers は待機中プレイヤーを DB から取得します。
func getWaitingPlayers(tx *sql.Tx) ([]Player, error) {
	query := "SELECT player_id, rating FROM matchmaking_queue ORDER BY waiting_since ASC FOR UPDATE"
	rows, err := tx.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var players []Player
	for rows.Next() {
		var p Player
		if err := rows.Scan(&p.ID, &p.Rating); err != nil {
			return nil, err
		}
		players = append(players, p)
	}
	return players, nil
}

// insertSession は生成したセッション情報を DB に登録します。
func insertSession(session SessionResult) error {
	query := "INSERT INTO sessions (session_id, player1_id, player2_id, start_time) VALUES (?, ?, ?, NOW())"
	_, err := db.Exec(query, session.SessionID, session.Player1.ID, session.Player2.ID)
	return err
}

// createSession は新しいセッションIDを生成してセッション結果を返します。
func createSession(p1, p2 Player) SessionResult {
	sessionID := fmt.Sprintf("session-%d", time.Now().UnixNano())
	return SessionResult{
		SessionID: sessionID,
		Player1:   p1,
		Player2:   p2,
	}
}

// matchmakingProcessor は別ゴルーチンで動作し、DB上の待機プレイヤーを定期的にチェックしてマッチングを実施します。
func matchmakingProcessor() {
	for {
		time.Sleep(1 * time.Second)

		// トランザクションを開始して、待機プレイヤーの一覧を取得（FOR UPDATEで排他制御）
		tx, err := db.Begin()
		if err != nil {
			log.Printf("matchmakingProcessor: トランザクション開始エラー: %v", err)
			continue
		}

		players, err := getWaitingPlayers(tx)
		if err != nil {
			log.Printf("matchmakingProcessor: 待機プレイヤー取得エラー: %v", err)
			tx.Rollback()
			continue
		}

		// マッチング可能なプレイヤーが２人以上いる場合、先頭2名をマッチング
		if len(players) >= 2 {
			p1 := players[0]
			p2 := players[1]
			session := createSession(p1, p2)

			// マッチング済みプレイヤーを待機キューから削除
			delQuery := "DELETE FROM matchmaking_queue WHERE player_id IN (?, ?)"
			if _, err := tx.Exec(delQuery, p1.ID, p2.ID); err != nil {
				log.Printf("matchmakingProcessor: 待機プレイヤー削除エラー: %v", err)
				tx.Rollback()
				continue
			}

			// セッション情報を DB に登録
			if err := insertSession(session); err != nil {
				log.Printf("matchmakingProcessor: セッション登録エラー: %v", err)
				tx.Rollback()
				continue
			}

			if err := tx.Commit(); err != nil {
				log.Printf("matchmakingProcessor: コミットエラー: %v", err)
				continue
			}
			log.Printf("Matched players %s and %s -> session %s", p1.ID, p2.ID, session.SessionID)

			// マッチング結果を保持しているチャネルへ通知する
			waitingChansMutex.Lock()
			if ch, ok := waitingChans[p1.ID]; ok {
				ch <- session
				delete(waitingChans, p1.ID)
			}
			if ch, ok := waitingChans[p2.ID]; ok {
				ch <- session
				delete(waitingChans, p2.ID)
			}
			waitingChansMutex.Unlock()
		} else {
			// マッチング可能なプレイヤーがいなければコミットして終了
			tx.Commit()
		}
	}
}

// matchmakingHandler は、プレイヤーの対戦開始リクエストを処理し、DBと in-memory の状態を更新します。
func matchmakingHandler(w http.ResponseWriter, r *http.Request) {
	var player Player
	if err := json.NewDecoder(r.Body).Decode(&player); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// まず DB に待機プレイヤーとして登録する
	if err := insertWaitingPlayer(player); err != nil {
		log.Printf("matchmakingHandler: DB登録エラー: %v", err)
		http.Error(w, "Failed to register waiting player", http.StatusInternalServerError)
		return
	}

	// マッチング結果を受け取るためのチャネルを作成し、in-memory マップに保存
	matchChan := make(chan SessionResult, 1)
	waitingChansMutex.Lock()
	waitingChans[player.ID] = matchChan
	waitingChansMutex.Unlock()

	log.Printf("Player %s registered for matchmaking", player.ID)

	// 30秒間、マッチング結果の通知を待つ
	select {
	case session := <-matchChan:
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(session); err != nil {
			log.Printf("matchmakingHandler: レスポンスエンコードエラー: %v", err)
		}
	case <-time.After(30 * time.Second):
		// タイムアウト時、in-memory からチャネルを削除し、DBからも待機プレイヤーを削除
		waitingChansMutex.Lock()
		delete(waitingChans, player.ID)
		waitingChansMutex.Unlock()

		if err := deleteWaitingPlayer(player.ID); err != nil {
			log.Printf("matchmakingHandler: タイムアウト時のDB削除エラー: %v", err)
		}
		http.Error(w, "No opponent found within timeout", http.StatusGatewayTimeout)
	}
}

// corsMiddleware はCORSのためのヘッダーを追加するミドルウェアです。
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 必要なヘッダーを設定
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// preflightリクエストの場合はここで終了
		if r.Method == "OPTIONS" {
			return
		}

		next.ServeHTTP(w, r)
	})
}

func main() {
	// DB初期化
	if err := initDB(); err != nil {
		log.Fatalf("DB初期化失敗: %v", err)
	}
	defer db.Close()

	// スキーマの初期化（外部ファイルから）
	if err := initSchemaFromFile("schema.sql"); err != nil {
		log.Fatalf("スキーマ初期化失敗: %v", err)
	}

	// マッチングプロセッサーを別ゴルーチンで起動
	go matchmakingProcessor()

	// ハンドラにCORSミドルウェアを適用
	http.Handle("/matchmaking", corsMiddleware(http.HandlerFunc(matchmakingHandler)))
	
	log.Println("Matchmaking service running on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
