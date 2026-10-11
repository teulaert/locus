package store

// LearnedPeer is a persisted discovery address book entry: a peer learned
// from discovery traffic, with the addresses it was last known at.
type LearnedPeer struct {
	PeerID        string
	DiscoveryAddr string
	SyncAddr      string
	Hostname      string
	// Via is the peer that reported the address, "" when observed directly.
	Via string
	// LastSeen is the last verified handshake with the peer itself (unix
	// seconds, 0 when only heard about); LearnedAt the last confirmation of
	// the address from any source.
	LastSeen  int64
	LearnedAt int64
}

// ListLearnedPeers returns every learned peer, ordered by peer ID.
func (s *Store) ListLearnedPeers() ([]LearnedPeer, error) {
	rows, err := s.State.Query(`SELECT peer_id, discovery_addr, sync_addr, hostname, via, last_seen, learned_at
		FROM learned_peers ORDER BY peer_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LearnedPeer
	for rows.Next() {
		var p LearnedPeer
		if err := rows.Scan(&p.PeerID, &p.DiscoveryAddr, &p.SyncAddr, &p.Hostname, &p.Via, &p.LastSeen, &p.LearnedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ReplaceLearnedPeers makes peers the complete set of learned peers, in one
// transaction.
func (s *Store) ReplaceLearnedPeers(peers []LearnedPeer) error {
	tx, err := s.State.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM learned_peers`); err != nil {
		return err
	}
	for _, p := range peers {
		if _, err := tx.Exec(`INSERT INTO learned_peers (peer_id, discovery_addr, sync_addr, hostname, via, last_seen, learned_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`, p.PeerID, p.DiscoveryAddr, p.SyncAddr, p.Hostname, p.Via, p.LastSeen, p.LearnedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}
