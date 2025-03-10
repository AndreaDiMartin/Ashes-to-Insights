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
