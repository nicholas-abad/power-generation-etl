# Data Units and Resolution Guide

This document explains the different units and resolutions used across the power generation data sources.

## Units Summary

| Source | Field Name | Unit | Meaning |
|--------|------------|------|---------|
| **ENTSOE** | `generation_mw` | MW (Megawatts) | Instantaneous power output |
| **NPP** | `generation_mwh` | MWh (Megawatt-hours) | Energy generated over period |
| **EIA** | `net_generation_mwh` | MWh (Megawatt-hours) | Net energy generated over period |

## Understanding MW vs MWh

### MW (Megawatts) - Power
- Measures **instantaneous power output** at a point in time
- Like the speedometer in a car showing current speed
- Used by ENTSOE because they report at 15-minute to hourly intervals
- Each data point represents the power level at that moment

### MWh (Megawatt-hours) - Energy
- Measures **total energy produced** over a time period
- Like the odometer showing total distance traveled
- Used by NPP (daily totals) and EIA (monthly totals)
- Formula: `Energy (MWh) = Power (MW) × Time (hours)`

## Data Resolution

| Source | Resolution | `resolution_minutes` | Description |
|--------|------------|---------------------|-------------|
| **ENTSOE** | 15-60 min | 15, 30, or 60 | Sub-hourly power readings |
| **NPP** | Daily | 1440 | Daily energy totals |
| **EIA** | Monthly | NULL | Monthly energy totals |

## Converting Between Units

### ENTSOE MW → MWh (for comparison)
To calculate energy from ENTSOE power readings:

```python
# For hourly data (resolution_minutes = 60)
energy_mwh = power_mw * 1.0  # 1 hour

# For 15-minute data (resolution_minutes = 15)
energy_mwh = power_mw * 0.25  # 0.25 hours

# General formula
energy_mwh = power_mw * (resolution_minutes / 60)
```

### Daily/Monthly Aggregation
To aggregate ENTSOE data to daily totals:

```python
# Sum energy over 24 hours
daily_mwh = df.groupby(['plant_name', 'date'])['energy_mwh'].sum()
```

## Why Different Units?

The data sources use different units because of their native reporting formats:

1. **ENTSOE**: Reports real-time/near-real-time power output from grid operators
   - Grid operators monitor instantaneous power for balancing
   - High-frequency data (every 15-60 minutes)

2. **NPP (India)**: Reports daily generation totals from power plants
   - Plants report accumulated energy production
   - Daily operational reports

3. **EIA (USA)**: Reports monthly generation from regulatory filings
   - Monthly EIA-923 forms report total generation
   - Regulatory compliance reporting

## Best Practices

1. **Don't directly compare** `generation_mw` with `generation_mwh` - they measure different things
2. **Convert to common units** before cross-source analysis
3. **Consider resolution** when aggregating - ENTSOE can be summed to daily/monthly, but NPP/EIA cannot be disaggregated
4. **Check `resolution_minutes`** field to understand data granularity

## Field Reference

### ENTSOE Fields
```json
{
  "generation_mw": 250.5,      // Power in MW at this timestamp
  "resolution_minutes": 60,    // Data point interval
  "timestamp_ms": 1704067200000
}
```

### NPP Fields
```json
{
  "generation_mwh": 1200.0,    // Energy in MWh for this day
  "resolution_minutes": 1440,  // Daily (24 * 60 minutes)
  "timestamp_ms": 1704067200000
}
```

### EIA Fields
```json
{
  "net_generation_mwh": 45000.0,  // Energy in MWh for this month
  "resolution_minutes": null,     // Monthly (no sub-monthly resolution)
  "timestamp_ms": 1704067200000
}
```
