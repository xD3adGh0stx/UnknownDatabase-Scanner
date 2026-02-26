==========================================
  UnknownDatabase Scanner - Gebruikersgids
==========================================

Wat is dit?
-----------
Een lokale zoekwebsite voor je database (.txt / NDJSON bestanden).
Werkt volledig offline op je eigen computer.
Snel, laag geheugengebruik. Zoek op naam, telefoon, e-mail, IBAN,
postcode, straat of klant-ID. Klik op een rij voor het volledige
klantdossier inclusief activiteitenlog en waarschuwingen.


STAP 1: Python installeren (eenmalig)
--------------------------------------
Python is al geinstalleerd op je systeem.
Als je het ooit opnieuw moet installeren:
1. Ga naar: https://www.python.org/downloads/
2. Download de Windows installer
3. Vink aan: "Add Python to PATH"


STAP 2: Database importeren (eenmalig per bestand)
---------------------------------------------------
  - Dubbelklik op menu.bat
  - Kies optie 2 (Database importeren)
  - Geef het volledige pad naar je .txt bestand op
  - Geef een naam voor de database (bijv. "Odido")

LET OP: Voor grote bestanden (miljoenen records) kan dit
        10-60 minuten duren. Laat het venster open staan.
        Dit hoeft maar EENMALIG gedaan te worden!

Je kunt meerdere databases importeren en tegelijk doorzoeken.


STAP 2b: Migreer een bestaande database (eenmalig)
---------------------------------------------------
Als je al een database.db hebt van een oudere versie:
  - Dubbelklik op menu.bat
  - Kies optie 4 (Database migreren)
  - Dit voegt de IBAN-kolom toe en extraheert telefoonnummers
    die eerder werden gemist uit het activiteitenlog

Dit is alleen nodig als je database.db al bestond voor de update.


STAP 3: Scanner starten
------------------------
  - Dubbelklik op menu.bat
  - Kies optie 1 (Scanner starten)
  - Je browser opent automatisch op http://localhost:3000
  - Typ een naam, telefoonnummer, e-mail, IBAN, postcode of
    klant-ID om te zoeken


Zoekmogelijkheden
-----------------
  Alle velden    - Zoek gelijktijdig in naam, telefoon, e-mail,
                   IBAN, postcode en straat
  Naam           - Zoek op voor- of achternaam
  Telefoon       - Zoek op (deel van) een telefoonnummer
  E-mail         - Zoek op (deel van) een e-mailadres
  IBAN           - Zoek op rekeningnummer (bijv. "NL77RABO")
  Stad           - Zoek op plaatsnaam
  Postcode       - Zoek op postcode (bijv. "1504" of "1504 HN")
  Straat         - Zoek op straatnaam
  Klant ID       - Zoek op Salesforce ID (exacte match)
  Notities / Log - Zoek in het volledige activiteitenlog en notities
                   (bijv. schuldhulpverlener, agressief, overlijden)


Klantdossier (detail-scherm)
-----------------------------
Klik op een rij in de zoekresultaten om het volledige klantdossier
te openen. Je ziet:

  - WAARSCHUWING banner (rood) als Flash_Message__c gevuld is
    (bijv. "KLANT IS AGRESSIEF", "OVERLEDEN")
  - Samenvatting met naam, telefoon, e-mail, IBAN en adres
  - Alle beschikbare velden gegroepeerd per categorie
  - Activiteitenlog als tijdlijn met per entry:
      * Datum en tijdstip
      * Richting (Inbound / Outbound)
      * Medewerker-ID
      * Tekst van de actie
    Rode entries = waarschuwingskeywords gevonden
    Blauwe entries = SMS verstuurd


Tips
----
  - Zoeken werkt op gedeeltelijke overeenkomsten
    ("jan" vindt "Jansen", "Jan de Boer")
  - Klik op een rij voor het volledige klantdossier
  - Exporteer resultaten naar CSV via de "Exporteer CSV" knop
  - De server draait lokaal, geen internetverbinding nodig
  - Druk op Escape of klik buiten het venster om het dossier te sluiten


Bestanden
---------
  menu.bat     - Het hoofdmenu (dubbelklik om te starten)
  server.py    - De webserver
  import.py    - Het importscript
  migrate.py   - Het migratiescript
  manage.py    - Database beheer (verwijderen/weergeven)
  index.html   - De zoekpagina (wordt geopend in browser)
  databases.json - Lijst van alle geïmporteerde databases
  *.db         - SQLite databases (aangemaakt na import)


Systeemvereisten
----------------
  - Windows 10 of 11
  - Python 3.10+ (al geinstalleerd op je systeem)
  - ~2x de grootte van je .txt bestand aan vrije schijfruimte
  - Laag geheugengebruik (<200 MB tijdens gebruik)


Problemen oplossen
------------------
  "database.db not found"   -> Kies optie 2 (Importeren) in menu.bat
  "Port 3000 in use"        -> Sluit het andere menu.bat venster
  Import duurt lang         -> Normaal voor grote bestanden, laat draaien
  Geen IBAN kolom           -> Kies optie 4 (Migreren) in menu.bat
  Python niet gevonden      -> Zie https://www.python.org/downloads/

==========================================
