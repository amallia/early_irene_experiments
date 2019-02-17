# Early Irene Experiments

See [jjfiv/irene](https://github.com/jjfiv/irene) instead.

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

