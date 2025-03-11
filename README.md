# Compilación
Primero hacer
```bash
mvn install
```
Luego, 
```bash
mvn compile
```
Para correr el serializer 
```bash
mvn -q exec:java -Dexec.mainClass=mapreduce.SpotifySerializer -Dexec.args="serializer"
```

Para correr el contador de tracks por año
```bash
mvn -q exec:java -Dexec.mainClass=mapreduce.YearCounterMapRed -Dexec.args="outputSerializado outputYearCounter"
```