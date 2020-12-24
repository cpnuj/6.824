#!/bin/sh

for var in {1..4}
do
    (go run mrworker.go wc.so &)
done
