# pprl-on-flink

PPRL on Flink Projekt im Rahmen des Big Data Praktikums der Abteilung Datenbanken der Universität Leipzig im Sommersemester 2016.

### Hinweise

- Zur Ausführung des PPRL-Prozesses dient die Klasse `PprlFlinkJob`
- Um das PPRL durchzuführen, muss ein neuer `PprlFlinkJob` angelegt werden
  - `PprlFlinkJob job = new PprlFlinkJob();` 
- Über setter-Methoden können die notwendigen Parameter gesetzt werden, diese sind:
  - `dataFilePath`: Pfad zur Datenquelle (txt/csv-Datei)
  - `dataFilePathDup`: Pfad zur Datenquelle mit den Duplikaten
  - `lineDelimiter`: Trennzeichen für die einzelnen Datensätze in den Dateien
  - `fieldDelimiter`: Trennzeichen für Attribute der Datensätze in den Dateien
  - `includingFields`: Zeichenkette bestehend aus Nullen und Einsen, wobei die Länge der Anzahl der Attribute eines Datensatzes entspricht. Steht an der Position i für Attribut i eine 1 (0), so wird dieses Attribut i gelesen (übersprungen)
  - `personFields`: String-Array mit den Person-Attributen (aus der Klasse Person) auf welche die zuvor gelesenen Attribute gemappt werden sollen
  - `ignoreFirstLine`: True, falls erste Zeile der Dateien ignoriert werden soll
  - `withCharacterPadding`: Nutzung eines Padding-Zeichens bei der Erstellung der n-Gramme (Token)
  - `bloomFilterBuildVariantOne`: 
  - `bloomFilterSize`: Länge der Bloom Filter
  - `bloomFilterHashes`: Anzahl der Hashfunktionen für die Bloom Filter
  - `ngramValue`: Größe der n-Gramme (Tokens)
  - `numberOfHashFamilies`: Anzahl der Hashfamilies (= Anzahl der LSH-Keys)
  - `numberOfHashesPerFamily`: Anzahl der Hashfunktionen pro Hashfamily (= Länge der LSH-Keys)
  - `comparisonThreshold`: Schwellwert für die Ähnlichkeitsberechnung
- Mit `job.runJob();` kann der PPRL-Job gestartet werden
- 

