// Copyright 2026 Supabase, Inc.
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
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPlusAuthenticator(t *testing.T, cbindHash []byte) (*ScramAuthenticator, *ScramHash) {
	t.Helper()
	hash := createTestHash("testpassword", []byte("plussalt12345678"), 4096)
	auth := NewScramAuthenticator(hash, "testdb")
	// Mirror the runtime: PLUS-capable authenticator is always over TLS.
	auth.SetOverTLS(true)
	auth.SetChannelBinding(&ChannelBinding{TLSServerEndPointHash: cbindHash})
	return auth, hash
}

func TestScramAuthenticator_StartAuthentication_AdvertisesPlus(t *testing.T) {
	t.Run("advertises PLUS first when binding present", func(t *testing.T) {
		auth, _ := newPlusAuthenticator(t, []byte("fake-cert-hash-32-bytes--------!!"))
		mechs := auth.StartAuthentication()
		require.Equal(t, []string{ScramSHA256PlusMechanism, ScramSHA256Mechanism}, mechs)
	})

	t.Run("advertises base only when binding absent", func(t *testing.T) {
		// Fresh authenticator: no binding set, no overTLS.
		hash := createTestHash("p", []byte("salt-16-bytes---"), 4096)
		auth := NewScramAuthenticator(hash, "testdb")
		mechs := auth.StartAuthentication()
		assert.Equal(t, []string{ScramSHA256Mechanism}, mechs)
	})

	t.Run("empty hash slice treated as no binding", func(t *testing.T) {
		auth, _ := newPlusAuthenticator(t, []byte{})
		mechs := auth.StartAuthentication()
		assert.Equal(t, []string{ScramSHA256Mechanism}, mechs)
	})
}

func TestScramAuthenticator_Plus_HappyPath(t *testing.T) {
	cbind := []byte("0123456789abcdef0123456789abcdef")
	auth, _ := newPlusAuthenticator(t, cbind)
	_ = auth.StartAuthentication()

	client := NewSCRAMClientWithPassword("testuser", "testpassword")
	client.EnableChannelBinding(cbind)

	clientFirst, err := client.ClientFirstMessage()
	require.NoError(t, err)
	require.Equal(t, ScramSHA256PlusMechanism, client.Mechanism())

	serverFirst, err := auth.HandleClientFirst(client.Mechanism(), clientFirst, "testuser")
	require.NoError(t, err)
	require.NotEmpty(t, serverFirst)

	clientFinal, err := client.ProcessServerFirst(serverFirst)
	require.NoError(t, err)

	serverFinal, err := auth.HandleClientFinal(clientFinal)
	require.NoError(t, err)
	require.NoError(t, client.VerifyServerFinal(serverFinal))
	require.True(t, auth.IsAuthenticated())
}

func TestScramAuthenticator_Plus_TamperedCBindRejected(t *testing.T) {
	serverCBind := []byte("server-side-cert-hash-32-bytes!!")
	clientCBind := []byte("attacker-supplied-hash-32-bytes!")

	auth, _ := newPlusAuthenticator(t, serverCBind)
	_ = auth.StartAuthentication()

	client := NewSCRAMClientWithPassword("testuser", "testpassword")
	client.EnableChannelBinding(clientCBind)

	clientFirst, err := client.ClientFirstMessage()
	require.NoError(t, err)

	serverFirst, err := auth.HandleClientFirst(client.Mechanism(), clientFirst, "testuser")
	require.NoError(t, err)

	clientFinal, err := client.ProcessServerFirst(serverFirst)
	require.NoError(t, err)

	_, err = auth.HandleClientFinal(clientFinal)
	require.ErrorIs(t, err, ErrChannelBindingCheck)
	require.False(t, auth.IsAuthenticated())
}

func TestScramAuthenticator_Plus_DowngradeYRejected(t *testing.T) {
	// Server has channel binding (TLS); client sends gs2 flag "y" claiming
	// the server doesn't support PLUS. Must fail per RFC 5802 §6.
	cbind := []byte("server-cert-hash-32-bytes------!!")
	auth, _ := newPlusAuthenticator(t, cbind)
	_ = auth.StartAuthentication()

	clientFirst := "y,,n=testuser,r=fyko+d2lbbFgONRv9qkxdawL"
	_, err := auth.HandleClientFirst(ScramSHA256Mechanism, clientFirst, "testuser")
	require.ErrorIs(t, err, ErrChannelBindingNegotiation)
}

func TestScramAuthenticator_Plus_PSelectsBaseMechanismRejected(t *testing.T) {
	auth, _ := newPlusAuthenticator(t, []byte("hash-32-bytes--------------------"))
	_ = auth.StartAuthentication()

	clientFirst := "p=tls-server-end-point,,n=testuser,r=fyko+d2lbbFgONRv9qkxdawL"
	_, err := auth.HandleClientFirst(ScramSHA256Mechanism, clientFirst, "testuser")
	require.ErrorIs(t, err, ErrSASLProtocol)
	var sp *SASLProtocolError
	require.ErrorAs(t, err, &sp)
	require.Contains(t, sp.Detail, "SCRAM-SHA-256 without channel binding")
}

func TestScramAuthenticator_Plus_NPlusMechanismRejected(t *testing.T) {
	auth, _ := newPlusAuthenticator(t, []byte("hash-32-bytes--------------------"))
	_ = auth.StartAuthentication()

	clientFirst := "n,,n=testuser,r=fyko+d2lbbFgONRv9qkxdawL"
	_, err := auth.HandleClientFirst(ScramSHA256PlusMechanism, clientFirst, "testuser")
	require.ErrorIs(t, err, ErrSASLProtocol)
	var sp *SASLProtocolError
	require.ErrorAs(t, err, &sp)
	require.Contains(t, sp.Detail, "does not include channel binding data")
}

func TestScramAuthenticator_AuthzidRejected(t *testing.T) {
	hash := createTestHash("p", []byte("salt-16-bytes---"), 4096)
	auth := NewScramAuthenticator(hash, "testdb")
	_ = auth.StartAuthentication()

	clientFirst := "n,a=other-user,n=testuser,r=nonce"
	_, err := auth.HandleClientFirst(ScramSHA256Mechanism, clientFirst, "testuser")
	require.ErrorIs(t, err, ErrAuthzidNotSupported)
}

func TestScramAuthenticator_Plus_PlusWithoutBindingMaterial(t *testing.T) {
	hash := createTestHash("p", []byte("salt-16-bytes---"), 4096)
	auth := NewScramAuthenticator(hash, "testdb")
	_ = auth.StartAuthentication()

	clientFirst := "p=tls-server-end-point,,n=testuser,r=nonce"
	_, err := auth.HandleClientFirst(ScramSHA256PlusMechanism, clientFirst, "testuser")
	require.Error(t, err)
	require.Contains(t, err.Error(), ScramSHA256PlusMechanism)
}

func TestScramAuthenticator_Plus_UnsupportedCBindType(t *testing.T) {
	cbind := []byte("hash-32-bytes--------------------")
	auth, _ := newPlusAuthenticator(t, cbind)
	_ = auth.StartAuthentication()

	clientFirst := "p=tls-unique,,n=testuser,r=nonce"
	_, err := auth.HandleClientFirst(ScramSHA256PlusMechanism, clientFirst, "testuser")
	require.ErrorIs(t, err, ErrSASLProtocol)
	var sp *SASLProtocolError
	require.ErrorAs(t, err, &sp)
	require.Contains(t, sp.Msg, "unsupported SCRAM channel-binding type")
	require.Contains(t, sp.Msg, "tls-unique")
}

func TestScramAuthenticator_Plus_DowngradeYRejectedWhenTLSButNoCbind(t *testing.T) {
	hash := createTestHash("testpassword", []byte("plain-salt-16byt"), 4096)
	auth := NewScramAuthenticator(hash, "testdb")
	auth.SetOverTLS(true) // TLS in use but no cbind material available.
	_ = auth.StartAuthentication()

	clientFirst := "y,,n=testuser,r=fyko+d2lbbFgONRv9qkxdawL"
	_, err := auth.HandleClientFirst(ScramSHA256Mechanism, clientFirst, "testuser")
	require.ErrorIs(t, err, ErrChannelBindingNegotiation)
}

func TestScramAuthenticator_SetChannelBinding_PanicsAfterStart(t *testing.T) {
	hash := createTestHash("p", []byte("salt-16-bytes---"), 4096)
	auth := NewScramAuthenticator(hash, "testdb")
	_ = auth.StartAuthentication()
	require.Panics(t, func() {
		auth.SetChannelBinding(&ChannelBinding{TLSServerEndPointHash: []byte("x")})
	})
}

func TestScramAuthenticator_SetOverTLS_PanicsAfterStart(t *testing.T) {
	hash := createTestHash("p", []byte("salt-16-bytes---"), 4096)
	auth := NewScramAuthenticator(hash, "testdb")
	_ = auth.StartAuthentication()
	require.Panics(t, func() {
		auth.SetOverTLS(true)
	})
}

func TestScramAuthenticator_Plaintext_NotAffectedByPlus(t *testing.T) {
	hash := createTestHash("testpassword", []byte("plain-salt-16byt"), 4096)
	auth := NewScramAuthenticator(hash, "testdb")
	mechs := auth.StartAuthentication()
	require.Equal(t, []string{ScramSHA256Mechanism}, mechs)

	// "y" gs2 flag without binding — must be accepted.
	yClientFirst := "y,,n=testuser,r=fyko+d2lbbFgONRv9qkxdawL"
	serverFirst, err := auth.HandleClientFirst(ScramSHA256Mechanism, yClientFirst, "testuser")
	require.NoError(t, err)
	require.NotEmpty(t, serverFirst)
}

func TestScramAuthenticator_Plus_CBindWiredCorrectly(t *testing.T) {
	cbind := []byte("specific-hash-32-bytes-----------")
	auth, hash := newPlusAuthenticator(t, cbind)
	_ = auth.StartAuthentication()

	clientFirst := "p=tls-server-end-point,,n=testuser,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j"
	serverFirst, err := auth.HandleClientFirst(ScramSHA256PlusMechanism, clientFirst, "testuser")
	require.NoError(t, err)

	combinedNonce := ""
	for part := range strings.SplitSeq(serverFirst, ",") {
		if strings.HasPrefix(part, "r=") {
			combinedNonce = part[2:]
		}
	}
	require.NotEmpty(t, combinedNonce)

	expectedCBindB64 := base64.StdEncoding.EncodeToString(append([]byte("p=tls-server-end-point,,"), cbind...))

	wrong := []byte(expectedCBindB64)
	wrong[0] ^= 0x01
	clientFinalBad := "c=" + string(wrong) + ",r=" + combinedNonce + ",p=" + base64.StdEncoding.EncodeToString(make([]byte, 32))
	_, err = auth.HandleClientFinal(clientFinalBad)
	require.ErrorIs(t, err, ErrChannelBindingCheck)

	// Sanity: matching cbind exits the cbind check (auth then fails on
	// password proof because we used zero bytes — different error class).
	auth.state = stateClientFirstReceived
	auth.hash = hash
	clientFinalGood := "c=" + expectedCBindB64 + ",r=" + combinedNonce + ",p=" + base64.StdEncoding.EncodeToString(make([]byte, 32))
	_, err = auth.HandleClientFinal(clientFinalGood)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrChannelBindingCheck)
}
