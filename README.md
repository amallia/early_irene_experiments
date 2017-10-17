# Supervised PRF

This repository contains a reproduction of the paper: [Selecting good expansion terms for pseudo-relevance feedback](https://doi.org/10.1145/1390334.1390377).

## Steps:

Get Maven, get Java 8.

### Checkout and install Galago

    git submodule update --init --recursive
    cd deps/galago
    ./scripts/installib.sh
    mvn install -DskipTests
    cd - # go back to root

### Compile this

    mvn install
    java -jar target/sprf-*.jar

