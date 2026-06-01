# Quick runner for indexer-pred
param(
    [switch]$MarketsOnly,
    [int]$MaxTradesPerMarket = 0
)

$argsList = @("fetch_events.py")
if ($MarketsOnly) { $argsList += "--markets-only" }
if ($MaxTradesPerMarket -gt 0) {
    $argsList += "--max-trades-per-market"
    $argsList += "$MaxTradesPerMarket"
}

python @argsList
