package com.example.batch;

import java.util.Collection;

public record YearReport(int year, Collection<YearPlatformSales> breakout) {
}
