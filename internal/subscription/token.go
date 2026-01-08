package subscription

import (
    "encoding/base64"
    "encoding/json"
    "errors"
    "time"
)

var ErrInvalidToken = errors.New("invalid subscription token")

// Encode zakodira grant
func Encode(grant *Grant) (string, error) {
    // pretvori json v byte -> {"UserID":42,"Expires":"2026-01-07T08:00:00Z", ...} (to so json bytes}
    data, err := json.Marshal(grant)
    if err != nil {
        return "", err
    }
    // zakodiraj byte v hash -> eyJVc2VySUQiOjQyLCJFeHBpcmVzIjoiMjAyNi0wMS0wN1QwODowMDowMFoifQ== ; ni presledkov in narekovaje ipd.
    return base64.StdEncoding.EncodeToString(data), nil
}

// Decode odkodira grant in preveri če se je iztekel
func Decode(token string) (*Grant, error) {
    raw, err := base64.StdEncoding.DecodeString(token)
    if err != nil {
        return nil, ErrInvalidToken
    }

    var g Grant
    if err := json.Unmarshal(raw, &g); err != nil {
        return nil, ErrInvalidToken
    }

    if time.Now().After(g.Expires) {
        return nil, ErrInvalidToken
    }

    return &g, nil
}

