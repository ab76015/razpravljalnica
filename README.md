# Razpravljalnica

Razpravljalnica je distribuirana spletna storitev za izmenjavo mnenj med uporabniki o različnih temah. Omogoča registracijo novih uporabnikov, dodajanje tem ter pošiljanje, urejanje in brisanje sporočil znotraj posameznih tem. Uporabniki lahko sledijo izbranim temam in sproti prejemajo obvestila o novih sporočilih. Prav tako je omogočeno všečkanje sporočil z beleženjem števila všečkov.

## Funkcionalnosti

- Registracija uporabnikov
- Ustvarjanje in seznam tem
- Pošiljanje, urejanje in brisanje sporočil
- Všečkanje sporočil z dinamičnim štetjem všečkov
- Naročanje na teme in prejemanje sporočil v realnem času
- Distribuirano delovanje z uporabo verižne replikacije za razporeditev bralnih zahtevkov
- Podpora odpovedim vozlišč z avtomatskim ponovnim vzpostavljanjem verige in usklajevanjem zapisov

## Tehnološki opis

Storitev je implementirana v programskem jeziku Go z uporabo gRPC za komunikacijo med strežnikom in odjemalci. Komunikacija med strežniškimi vozlišči je izvedena preko RPC protokola. Vsa podatkovna struktura in vmesnik so definirani v Protocol Buffers (`.proto`) datotekah, iz katerih je samodejno generirana Go koda za strežniški in odjemalski vmesnik.

## Struktura projekta

razpravljalnica/
├── api/
│ ├── proto/ # Definicija gRPC in Protocol Buffers vmesnika
│ └── pb/ # Samodejno generirana Go koda iz .proto
│
├── cmd/
│ ├── server/ # Izvršljiva koda strežnika (backend)
│ └── client/ # Odjemalec za testiranje in interakcijo s strežnikom
│
├── internal/ # Implementacija poslovne logike in pomožnih paketov
└── go.mod # Modul za upravljanje odvisnosti


## Navodila za razvoj in zagon

1. Namesti [protoc compiler](https://protobuf.dev/) in Go plugin za proto [grpc-go](https://grpc.io/docs/languages/go/quickstart/).
2. Generiraj Go kodo iz `.proto` datotek z ukazom `protoc`.
3. Zaženi strežnik z `go run cmd/server/main.go` (privzeto na portu `:50051`).
4. Zaženi odjemalca z `go run cmd/client/main.go` za testiranje RPC klicev.
5. Razvijaj dodatne funkcionalnosti znotraj `internal` paketov.
