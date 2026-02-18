// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scram

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	// ScramSHA256Mechanism is the SASL mechanism name for SCRAM-SHA-256.
	ScramSHA256Mechanism = "SCRAM-SHA-256"

	// serverNonceLength is the number of random bytes to add to the client nonce.
	serverNonceLength = 18
)

// clientFirstMessage represents a parsed SCRAM client-first-message.
// Format: gs2-header client-first-message-bare
// Where gs2-header = gs2-cbind-flag "," [authzid] ","
// And client-first-message-bare = username "," nonce ["," extensions]
type clientFirstMessage struct {
	// gs2CbindFlag is the channel binding flag ('n', 'y', or 'p').
	// 'n' = client doesn't support channel binding
	// 'y' = client supports but server doesn't advertise
	// 'p' = client requires channel binding
	gs2CbindFlag string

	// authzid is the optional authorization identity.
	authzid string

	// username is the authentication identity (saslprep normalized).
	username string

	// clientNonce is the client-generated random nonce.
	clientNonce string

	// clientFirstMessageBare is the message without the GS2 header.
	// This is needed for computing the AuthMessage.
	clientFirstMessageBare string
}

// clientFinalMessage represents a parsed SCRAM client-final-message.
// Format: channel-binding "," nonce "," proof
type clientFinalMessage struct {
	// channelBinding is the base64-encoded channel binding data.
	// For no channel binding, this is "biws" (base64 of "n,,").
	channelBinding string

	// nonce is the combined client+server nonce.
	nonce string

	// proof is the decoded client proof.
	proof []byte

	// clientFinalMessageWithoutProof is the message without the proof.
	// This is needed for computing the AuthMessage.
	clientFinalMessageWithoutProof string
}

// parseClientFirstMessage parses a SCRAM client-first-message.
// The message format is: gs2-cbind-flag "," [authzid] "," saslname "=" username "," "r=" nonce
// Example: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"
func parseClientFirstMessage(msg string) (*clientFirstMessage, error) {
	if msg == "" {
		return nil, errors.New("empty client-first-message")
	}

	// Split by comma to get parts.
	// Format: gs2-cbind-flag, [authzid], client-first-message-bare-parts...
	parts := strings.SplitN(msg, ",", 3)
	if len(parts) < 3 {
		return nil, errors.New("invalid client-first-message: expected at least 3 comma-separated parts")
	}

	// Parse GS2 header.
	gs2CbindFlag := parts[0]
	switch {
	case gs2CbindFlag == "n":
		// Client doesn't support channel binding.
	case gs2CbindFlag == "y":
		// Client supports channel binding but thinks server doesn't.
	case strings.HasPrefix(gs2CbindFlag, "p="):
		// Client requires channel binding.
		return nil, fmt.Errorf("channel binding not supported (client requested %q)", gs2CbindFlag)
	default:
		return nil, fmt.Errorf("invalid GS2 channel binding flag: %q", gs2CbindFlag)
	}

	// Parse optional authzid.
	var authzid string
	authzidPart := parts[1]
	if strings.HasPrefix(authzidPart, "a=") {
		authzid = authzidPart[2:]
	} else if authzidPart != "" {
		return nil, fmt.Errorf("invalid authzid part: %q", authzidPart)
	}

	// The rest is the client-first-message-bare.
	clientFirstMessageBare := parts[2]

	// Parse the client-first-message-bare for username and nonce.
	var username, clientNonce string
	for attr := range strings.SplitSeq(clientFirstMessageBare, ",") {
		if strings.HasPrefix(attr, "n=") {
			username = decodeSaslName(attr[2:])
		} else if strings.HasPrefix(attr, "r=") {
			clientNonce = attr[2:]
		}
		// Ignore other attributes (extensions).
	}

	if clientNonce == "" {
		return nil, errors.New("missing nonce in client-first-message")
	}

	return &clientFirstMessage{
		gs2CbindFlag:           gs2CbindFlag,
		authzid:                authzid,
		username:               username,
		clientNonce:            clientNonce,
		clientFirstMessageBare: clientFirstMessageBare,
	}, nil
}

// parseClientFinalMessage parses a SCRAM client-final-message.
// The message format is: "c=" channel-binding "," "r=" nonce "," "p=" proof
// Example: "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="
func parseClientFinalMessage(msg string) (*clientFinalMessage, error) {
	if msg == "" {
		return nil, errors.New("empty client-final-message")
	}

	// Parse attributes.
	var channelBinding, nonce, proofB64 string
	for attr := range strings.SplitSeq(msg, ",") {
		switch {
		case strings.HasPrefix(attr, "c="):
			channelBinding = attr[2:]
		case strings.HasPrefix(attr, "r="):
			nonce = attr[2:]
		case strings.HasPrefix(attr, "p="):
			proofB64 = attr[2:]
		}
	}

	if channelBinding == "" {
		return nil, errors.New("missing channel binding in client-final-message")
	}
	if nonce == "" {
		return nil, errors.New("missing nonce in client-final-message")
	}
	if proofB64 == "" {
		return nil, errors.New("missing proof in client-final-message")
	}

	// Decode the proof.
	proof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil {
		return nil, fmt.Errorf("invalid proof (base64 decode failed): %w", err)
	}

	// Build client-final-message-without-proof for AuthMessage computation.
	// Find where ",p=" starts and take everything before it.
	proofIdx := strings.LastIndex(msg, ",p=")
	if proofIdx == -1 {
		return nil, errors.New("malformed client-final-message: cannot find proof separator")
	}
	clientFinalMessageWithoutProof := msg[:proofIdx]

	return &clientFinalMessage{
		channelBinding:                 channelBinding,
		nonce:                          nonce,
		proof:                          proof,
		clientFinalMessageWithoutProof: clientFinalMessageWithoutProof,
	}, nil
}

// generateServerFirstMessage generates a SCRAM server-first-message.
// Returns the message string, the combined nonce, and any error.
// The message format is: "r=" nonce "," "s=" salt "," "i=" iteration-count
func generateServerFirstMessage(clientNonce string, salt []byte, iterations int) (string, string, error) {
	if clientNonce == "" {
		return "", "", errors.New("client nonce cannot be empty")
	}
	if len(salt) == 0 {
		return "", "", errors.New("salt cannot be empty")
	}
	if iterations <= 0 {
		return "", "", errors.New("iterations must be positive")
	}

	// Generate server nonce (random bytes, base64 encoded to be printable).
	serverNonceBytes := make([]byte, serverNonceLength)
	if _, err := rand.Read(serverNonceBytes); err != nil {
		return "", "", fmt.Errorf("failed to generate server nonce: %w", err)
	}
	serverNoncePart := base64.StdEncoding.EncodeToString(serverNonceBytes)

	// Combined nonce = client nonce + server nonce
	combinedNonce := clientNonce + serverNoncePart

	// Encode salt as base64.
	saltB64 := base64.StdEncoding.EncodeToString(salt)

	// Build the message.
	msg := fmt.Sprintf("r=%s,s=%s,i=%d", combinedNonce, saltB64, iterations)

	return msg, combinedNonce, nil
}

// generateServerFinalMessage generates a SCRAM server-final-message.
// The message format is: "v=" base64(server-signature)
func generateServerFinalMessage(serverSignature []byte) string {
	return "v=" + base64.StdEncoding.EncodeToString(serverSignature)
}

// decodeSaslName decodes a SASL-encoded username.
// In SASL names, '=' is encoded as '=3D' and ',' is encoded as '=2C'.
func decodeSaslName(s string) string {
	s = strings.ReplaceAll(s, "=2C", ",")
	s = strings.ReplaceAll(s, "=3D", "=")
	return s
}

// encodeSaslName encodes a username for SASL.
// '=' must be encoded as '=3D' and ',' must be encoded as '=2C'.
func encodeSaslName(s string) string {
	s = strings.ReplaceAll(s, "=", "=3D")
	s = strings.ReplaceAll(s, ",", "=2C")
	return s
}

// parseServerFirstMessage parses a SCRAM server-first-message.
// This is useful for client-side implementations and testing.
// The message format is: "r=" nonce "," "s=" salt "," "i=" iteration-count
func parseServerFirstMessage(msg string) (nonce string, salt []byte, iterations int, err error) {
	if msg == "" {
		return "", nil, 0, errors.New("empty server-first-message")
	}

	for attr := range strings.SplitSeq(msg, ",") {
		switch {
		case strings.HasPrefix(attr, "r="):
			nonce = attr[2:]
		case strings.HasPrefix(attr, "s="):
			salt, err = base64.StdEncoding.DecodeString(attr[2:])
			if err != nil {
				return "", nil, 0, fmt.Errorf("invalid salt: %w", err)
			}
		case strings.HasPrefix(attr, "i="):
			iterations, err = strconv.Atoi(attr[2:])
			if err != nil {
				return "", nil, 0, fmt.Errorf("invalid iterations: %w", err)
			}
		}
	}

	if nonce == "" {
		return "", nil, 0, errors.New("missing nonce in server-first-message")
	}
	if salt == nil {
		return "", nil, 0, errors.New("missing salt in server-first-message")
	}
	if iterations == 0 {
		return "", nil, 0, errors.New("missing iterations in server-first-message")
	}

	return nonce, salt, iterations, nil
}
