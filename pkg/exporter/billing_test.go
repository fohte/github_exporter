package exporter

import "testing"

func TestAggregateUsage(t *testing.T) {
	items := []UsageItem{
		{
			Type: "org", Name: "example", Product: "actions", SKU: "Actions Linux",
			UnitType: "Minutes", OrganizationName: "example", RepositoryName: "repo-a",
			Date: "2026-08-01T00:00:00Z", Quantity: 10, GrossAmount: 100, DiscountAmount: 5, NetAmount: 95,
		},
		{
			Type: "org", Name: "example", Product: "actions", SKU: "Actions Linux",
			UnitType: "Minutes", OrganizationName: "example", RepositoryName: "repo-a",
			Date: "2026-08-02T00:00:00Z", Quantity: 20, GrossAmount: 200, DiscountAmount: 10, NetAmount: 190,
		},
		{
			Type: "org", Name: "example", Product: "actions", SKU: "Actions Linux",
			UnitType: "Minutes", OrganizationName: "example", RepositoryName: "repo-b",
			Date: "2026-08-01T00:00:00Z", Quantity: 5, GrossAmount: 50, DiscountAmount: 0, NetAmount: 50,
		},
	}

	aggregated := aggregateUsage(items)

	if len(aggregated) != 2 {
		t.Fatalf("expected 2 aggregated series, got %d", len(aggregated))
	}

	keyA := billingUsageKey{
		Type: "org", Name: "example", Product: "actions", SKU: "Actions Linux",
		Unit: "Minutes", Org: "example", Repo: "repo-a",
	}

	totalsA, ok := aggregated[keyA]
	if !ok {
		t.Fatalf("expected aggregated totals for %+v", keyA)
	}

	if totalsA.Quantity != 30 {
		t.Errorf("expected quantity 30, got %v", totalsA.Quantity)
	}
	if totalsA.GrossAmount != 300 {
		t.Errorf("expected gross amount 300, got %v", totalsA.GrossAmount)
	}
	if totalsA.DiscountAmount != 15 {
		t.Errorf("expected discount amount 15, got %v", totalsA.DiscountAmount)
	}
	if totalsA.NetAmount != 285 {
		t.Errorf("expected net amount 285, got %v", totalsA.NetAmount)
	}
	if got, want := totalsA.PricePerUnit(), 10.0; got != want {
		t.Errorf("expected price per unit %v, got %v", want, got)
	}
}

func TestBillingUsageTotalsPricePerUnitZeroQuantity(t *testing.T) {
	if got := (billingUsageTotals{}).PricePerUnit(); got != 0 {
		t.Errorf("expected price per unit 0 for zero quantity, got %v", got)
	}
}
