# pprl-on-flink

PPRL on Flink Projekt im Rahmen des Big Data Praktikums der Abteilung Datenbanken der Universität Leipzig im Sommersemester 2016.

## Hinweise

#### Ausführung

- Zur Ausführung des PPRL-Prozesses dient die Klasse `PprlFlinkJob`
- Um das PPRL durchzuführen, muss ein neuer `PprlFlinkJob` angelegt werden
  - `PprlFlinkJob job = new PprlFlinkJob();` 
- Über setter-Methoden können die notwendigen Parameter gesetzt werden, diese sind:
  - `dataFilePath`: Pfad zur Datenquelle (txt/csv-Datei)
  - `dataFilePathDup`: Pfad zur Datenquelle mit den Duplikaten
  - `lineDelimiter`: Trennzeichen für die einzelnen Datensätze in den Dateien
  - `fieldDelimiter`: Trennzeichen für Attribute der Datensätze in den Dateien
  - `includingFields`: Zeichenkette bestehend aus Nullen und Einsen, wobei die Länge der Anzahl der Attribute eines Datensatzes entspricht. Steht an der Position i für Attribut i eine 1 (0), so wird dieses Attribut i gelesen (übersprungen)
  - `personFields`: String-Array mit den Person-Attributen (aus der Klasse `Person`) auf welche die zuvor gelesenen Attribute gemappt werden sollen
  - `ignoreFirstLine`: True, falls erste Zeile der Dateien ignoriert werden soll
  - `withCharacterPadding`: Nutzung eines Padding-Zeichens bei der Erstellung der n-Gramme (Token)
  - `bloomFilterBuildVariantRecordParallelism`: True -> Erstellung der Bloom Filter mit Parallelisierung auf Record-Ebene. False -> Parallelisierung auf Token-Ebene
  - `bloomFilterSize`: Länge der Bloom Filter
  - `bloomFilterHashes`: Anzahl der Hashfunktionen für die Bloom Filter
  - `ngramValue`: Größe der n-Gramme (Tokens)
  - `numberOfHashFamilies`: Anzahl der Hashfamilies (= Anzahl der LSH-Keys)
  - `numberOfHashesPerFamily`: Anzahl der Hashfunktionen pro Hashfamily (= Länge der LSH-Keys)
  - `comparisonThreshold`: Schwellwert für die Ähnlichkeitsberechnung
- Mit `job.runJob();` kann der PPRL-Job gestartet werden

#### PPRL-Prozess

- Der PPRL-Prozess besteht aus 6 Teilschritten, die Ergebnisse für jeden Teilschritt werden im Ordner *output_files* gespeichert
- Die Teilschritte sind:
  1. Auslesen der Datenquellen und Übertragung der Datensätze mit den entsprechenden Attributen in `Person`-Objekte
  2. Erstellung der n-Gramme (Tokens) und und daraus die enstprechenden Bloom Filter
  3. Berechnung der LSH-Keys
  4. Erstellung von Kandidaten-Record-Paaren für Objekte mit den selben LSH-Key-Wert für den selben LSH-Key
  5. Entfernung von doppelten Kandidaten-Record-Paaren
  6. Berechnung der Ähnlichkeit der Kandidaten-Record-Paare und Ausgabe der Matching Pairs

#### Paketstruktur

- `dbs.bigdata.flink.pprl.data`: Enthält Klassen zum Laden und Speichern der Daten als `Person`-Objekte
- `dbs.bigdata.flink.pprl.functions`: Enthält alle Klassen, welche Flink-Operationen realisieren und zur Transformation der Daten eingesetzt werden
- `dbs.bigdata.flink.pprl.job`: Enthält die Klasse `PprlFlinkJob`
- `dbs.bigdata.flink.pprl.utils`: Enthält Klassen zur Realisierung von LSH und der Bloom Filter
