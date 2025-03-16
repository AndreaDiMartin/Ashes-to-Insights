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

## Análisis de los albumes

Para correr el contador de Albums por año (El nombre de salida puede ser distinto)
```bash
mvn -q exec:java -Dexec.mainClass=mapreduce.AlbumCounterMapRed -Dexec.args="outputSerializado outputAlbumCounterMapRed"
```

Para correr el contador de Albums de cada mes por año (El nombre de salida puede ser distinto) 
```bash
mvn -q exec:java -Dexec.mainClass=mapreduce.AlbumsPerMonth -Dexec.args="outputSerializado outputAlbumsPerMonth"
```

Para correr un resumen del análisis por mes de cada año, el input debe ser la carpeta 
de salida generada anteriormente. 

```bash
sudo mvn -q exec:java -Dexec.mainClass=mapreduce.AlbumsPerMonthSummary -Dexec.args="outputAlbumsPerMonth outputAlbumsPerMonthSummary"
```

Para correr el mapReduce que muestra los meses que tienen mayores lanzamientos de albums
```bash
mvn -q exec:java -Dexec.mainClass=mapreduce.MaxPublicationMonthPerYear -Dexec.args="outputAlbumsPerMonthSummary outputMaxPublicationMonthPerYear" 
```

Para correr el contador de lanzamientos de albumes por días de cada año
```bash
mvn -e exec:java -Dexec.mainClass=mapreduce.AlbumsPerDay -Dexec.args="outputSerializado outputAlbumsPerDay"
```

Para correr el contador de albumes lanzados por día de la semana por año
```bash
mvn -q exec:java -Dexec.mainClass=mapreduce.AlbumsPerWeekdayPerYear -Dexec.args="outputSerializado outputAlbumsPerWeekdayPerYear"
```

Para correr el analizador por año de la popularidad de los albumes
```bash
mvn -e exec:java -Dexec.mainClass=mapreduce.AlbumPopularityAnalysis -Dexec.args="outputSerializado outputAlbumPopularityAnalysis"
```

