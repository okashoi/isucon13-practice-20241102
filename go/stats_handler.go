package main

import (
	"database/sql"
	"errors"
	"net/http"
	"sort"
	"strconv"

	"github.com/labstack/echo/v4"
)

type LivestreamStatistics struct {
	Rank           int64 `json:"rank"`
	ViewersCount   int64 `json:"viewers_count"`
	TotalReactions int64 `json:"total_reactions"`
	TotalReports   int64 `json:"total_reports"`
	MaxTip         int64 `json:"max_tip"`
}

type LivestreamRankingEntry struct {
	LivestreamID int64
	Score        int64
}
type LivestreamRanking []LivestreamRankingEntry

func (r LivestreamRanking) Len() int      { return len(r) }
func (r LivestreamRanking) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r LivestreamRanking) Less(i, j int) bool {
	if r[i].Score == r[j].Score {
		return r[i].LivestreamID < r[j].LivestreamID
	} else {
		return r[i].Score < r[j].Score
	}
}

type UserStatistics struct {
	Rank              int64  `json:"rank"`
	ViewersCount      int64  `json:"viewers_count"`
	TotalReactions    int64  `json:"total_reactions"`
	TotalLivecomments int64  `json:"total_livecomments"`
	TotalTip          int64  `json:"total_tip"`
	FavoriteEmoji     string `json:"favorite_emoji"`
}

type UserRankingEntry struct {
	Username string
	Score    int64
}
type UserRanking []UserRankingEntry

func (r UserRanking) Len() int      { return len(r) }
func (r UserRanking) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r UserRanking) Less(i, j int) bool {
	if r[i].Score == r[j].Score {
		return r[i].Username < r[j].Username
	} else {
		return r[i].Score < r[j].Score
	}
}

func getUserStatisticsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	username := c.Param("username")

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	var user UserModel
	if err := tx.GetContext(ctx, &user, "SELECT * FROM users WHERE name = ?", username); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusBadRequest, "not found user that has the given username")
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get user: "+err.Error())
		}
	}

	// Get all stats in a single query
	var stats struct {
		ReactionsCount    int64  `db:"reactions_count"`
		LivecommentsCount int64  `db:"livecomments_count"`
		TotalTip          int64  `db:"total_tip"`
		ViewersCount      int64  `db:"viewers_count"`
		FavoriteEmoji     string `db:"favorite_emoji"`
	}

	statsQuery := `
		SELECT 
			IFNULL(r.reactions_count, 0) as reactions_count,
			IFNULL(lc.livecomments_count, 0) as livecomments_count,
			IFNULL(lc.total_tip, 0) as total_tip,
			IFNULL(v.viewers_count, 0) as viewers_count,
			IFNULL(e.emoji_name, '') as favorite_emoji
		FROM users u
		LEFT JOIN (
			SELECT l.user_id, COUNT(*) as reactions_count
			FROM livestreams l
			INNER JOIN reactions r ON r.livestream_id = l.id
			GROUP BY l.user_id
		) r ON r.user_id = u.id
		LEFT JOIN (
			SELECT l.user_id, 
				   COUNT(*) as livecomments_count,
				   SUM(lc.tip) as total_tip
			FROM livestreams l
			INNER JOIN livecomments lc ON lc.livestream_id = l.id
			GROUP BY l.user_id
		) lc ON lc.user_id = u.id
		LEFT JOIN (
			SELECT l.user_id, COUNT(*) as viewers_count
			FROM livestreams l
			INNER JOIN livestream_viewers_history vh ON vh.livestream_id = l.id
			GROUP BY l.user_id
		) v ON v.user_id = u.id
		LEFT JOIN (
			SELECT t.user_id, t.emoji_name
			FROM (
				SELECT l.user_id, r.emoji_name, COUNT(*) as emoji_count,
					   ROW_NUMBER() OVER (PARTITION BY l.user_id ORDER BY COUNT(*) DESC, r.emoji_name DESC) as rn
				FROM livestreams l
				INNER JOIN reactions r ON r.livestream_id = l.id
				GROUP BY l.user_id, r.emoji_name
			) t WHERE t.rn = 1
		) e ON e.user_id = u.id
		WHERE u.id = ?
	`
	if err := tx.GetContext(ctx, &stats, statsQuery, user.ID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get statistics: "+err.Error())
	}

	// Calculate rank using window functions
	var rank int64
	rankQuery := `
		WITH user_scores AS (
			SELECT 
				u.id,
				u.name,
				IFNULL(SUM(r.reaction_count), 0) + IFNULL(SUM(lc.tip_sum), 0) as score
			FROM users u
			LEFT JOIN (
				SELECT l.user_id, COUNT(*) as reaction_count
				FROM livestreams l
				INNER JOIN reactions r ON r.livestream_id = l.id
				GROUP BY l.user_id
			) r ON r.user_id = u.id
			LEFT JOIN (
				SELECT l.user_id, SUM(lc.tip) as tip_sum
				FROM livestreams l
				INNER JOIN livecomments lc ON lc.livestream_id = l.id
				GROUP BY l.user_id
			) lc ON lc.user_id = u.id
			GROUP BY u.id, u.name
		)
		SELECT RANK() OVER (ORDER BY score DESC) as rank
		FROM user_scores
		WHERE name = ?
	`
	if err := tx.GetContext(ctx, &rank, rankQuery, username); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to calculate rank: "+err.Error())
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusOK, UserStatistics{
		Rank:              rank,
		ViewersCount:      stats.ViewersCount,
		TotalReactions:    stats.ReactionsCount,
		TotalLivecomments: stats.LivecommentsCount,
		TotalTip:          stats.TotalTip,
		FavoriteEmoji:     stats.FavoriteEmoji,
	})
}

func getLivestreamStatisticsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	id, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}
	livestreamID := int64(id)

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	var livestream LivestreamModel
	if err := tx.GetContext(ctx, &livestream, "SELECT * FROM livestreams WHERE id = ?", livestreamID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusBadRequest, "cannot get stats of not found livestream")
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream: "+err.Error())
		}
	}

	var livestreams []*LivestreamModel
	if err := tx.SelectContext(ctx, &livestreams, "SELECT * FROM livestreams"); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
	}

	// ランク算出
	var ranking LivestreamRanking
	for _, livestream := range livestreams {
		var reactions int64
		if err := tx.GetContext(ctx, &reactions, "SELECT COUNT(*) FROM livestreams l INNER JOIN reactions r ON l.id = r.livestream_id WHERE l.id = ?", livestream.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to count reactions: "+err.Error())
		}

		var totalTips int64
		if err := tx.GetContext(ctx, &totalTips, "SELECT IFNULL(SUM(l2.tip), 0) FROM livestreams l INNER JOIN livecomments l2 ON l.id = l2.livestream_id WHERE l.id = ?", livestream.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to count tips: "+err.Error())
		}

		score := reactions + totalTips
		ranking = append(ranking, LivestreamRankingEntry{
			LivestreamID: livestream.ID,
			Score:        score,
		})
	}
	sort.Sort(ranking)

	var rank int64 = 1
	for i := len(ranking) - 1; i >= 0; i-- {
		entry := ranking[i]
		if entry.LivestreamID == livestreamID {
			break
		}
		rank++
	}

	// 視聴者数算出
	var viewersCount int64
	if err := tx.GetContext(ctx, &viewersCount, `SELECT COUNT(*) FROM livestreams l INNER JOIN livestream_viewers_history h ON h.livestream_id = l.id WHERE l.id = ?`, livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count livestream viewers: "+err.Error())
	}

	// 最大チップ額
	var maxTip int64
	if err := tx.GetContext(ctx, &maxTip, `SELECT IFNULL(MAX(tip), 0) FROM livestreams l INNER JOIN livecomments l2 ON l2.livestream_id = l.id WHERE l.id = ?`, livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to find maximum tip livecomment: "+err.Error())
	}

	// リアクション数
	var totalReactions int64
	if err := tx.GetContext(ctx, &totalReactions, "SELECT COUNT(*) FROM livestreams l INNER JOIN reactions r ON r.livestream_id = l.id WHERE l.id = ?", livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count total reactions: "+err.Error())
	}

	// スパム報告数
	var totalReports int64
	if err := tx.GetContext(ctx, &totalReports, `SELECT COUNT(*) FROM livestreams l INNER JOIN livecomment_reports r ON r.livestream_id = l.id WHERE l.id = ?`, livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count total spam reports: "+err.Error())
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusOK, LivestreamStatistics{
		Rank:           rank,
		ViewersCount:   viewersCount,
		MaxTip:         maxTip,
		TotalReactions: totalReactions,
		TotalReports:   totalReports,
	})
}
