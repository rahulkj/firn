package api

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/rahulkj/firn/internal/apikeys"
	"github.com/rahulkj/firn/internal/ratelimit"
)

// Mesh listener authz split: ReplicaService/QuorumService are mTLS-only
// (nothing but cluster certs can reach them — that is enough), and
// FederationService additionally requires a federation-scoped API key, as
// documented in ADR-010/011 and the API reference.
func TestMeshUnaryInterceptorSplitsAuth(t *testing.T) {
	dir := t.TempDir()
	store, err := apikeys.OpenDir(dir)
	if err != nil {
		t.Fatalf("apikeys store: %v", err)
	}
	fedKey, err := store.Create("fed-peer", "federation")
	if err != nil {
		t.Fatalf("create federation key: %v", err)
	}
	opKey, err := store.Create("ops", "operator")
	if err != nil {
		t.Fatalf("create operator key: %v", err)
	}
	auth := NewAuthorizer(store, ratelimit.New(1000, 1000), nil)

	call := func(t *testing.T, method string, md metadata.MD) error {
		t.Helper()
		ctx := metadata.NewIncomingContext(context.Background(), md)
		called := false
		_, err := auth.MeshUnaryInterceptor()(ctx, &struct{}{},
			&grpc.UnaryServerInfo{FullMethod: method},
			func(ctx context.Context, req any) (any, error) { called = true; return "ok", nil })
		if err != nil {
			return err
		}
		if !called {
			t.Fatal("handler was not called")
		}
		return nil
	}

	// ReplicaService/QuorumService: mTLS is the auth — no key needed.
	if err := call(t, "/firn.v1.ReplicaService/ReplicaFetch", nil); err != nil {
		t.Fatalf("replica path without key: %v", err)
	}
	if err := call(t, "/firn.v1.QuorumService/QuorumMessage", nil); err != nil {
		t.Fatalf("quorum path without key: %v", err)
	}
	// FederationService with a valid federation key: passes.
	if err := call(t, "/firn.v1.FederationService/FederationFetch",
		metadata.New(map[string]string{"authorization": "Bearer " + fedKey})); err != nil {
		t.Fatalf("federation path with key: %v", err)
	}
	// FederationService without a key: UNAUTHENTICATED.
	if err := call(t, "/firn.v1.FederationService/FederationFetch", nil); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("federation path without key: want Unauthenticated, got %v", err)
	}
	// FederationService with a non-federation key: PERMISSION_DENIED.
	if err := call(t, "/firn.v1.FederationService/FederationFetch",
		metadata.New(map[string]string{"authorization": "Bearer " + opKey})); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("federation path with operator key: want PermissionDenied, got %v", err)
	}
}
