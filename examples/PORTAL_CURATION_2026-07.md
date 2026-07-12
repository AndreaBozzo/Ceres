# Portal curation — July 2026

This note records the curation pass performed after metadata-only probes of 190
new portals (162 ArcGIS Hub and 28 OpenDataSoft). It is intentionally separate
from `portals.toml`: that file contains portals selected for ongoing maintenance,
while this note preserves the decisions behind exclusions and the watchlist.

## Decision rules

- Keep broad, public, institutional catalogs that completed a full metadata-only
  harvest without dataset failures.
- Prefer canonical custom domains and parent catalogs over language aliases,
  development domains, project microsites, and near-complete child catalogs.
- Keep all source metadata, but do not treat an ArcGIS service URL as a file
  download.
- Reject ArcGIS Hub sites with an empty `catalogV2` item scope: those endpoints
  return global ArcGIS search results rather than portal-owned content.
- Leave all curated additions disabled by default until they have passed another
  scheduled incremental sync.

## Stabilized

Seventy-eight new portals were added to `portals.toml`: 27 OpenDataSoft
catalogs, 36 ArcGIS Hub catalogs, eleven CKAN catalogs, three Project Open Data
feeds (including two DKAN catalogs reused through the existing `static_json`
profile), and the Slovak national SPARQL DCAT catalogue.
Together with the previously configured
Washington DC and Utah Hub catalogs, the maintained sample covers national,
state, regional, and municipal publishers across North America, Europe, Asia,
Africa, the Middle East, Australia, and New Zealand.

## Definite exclusions

- `https://issy-les-moulineaux.opendatasoft.com` — exact alias of
  `https://data.issy.com` (328/328 item ids overlap).
- `https://catalogue-fr-SaintJohn.opendata.arcgis.com` — language alias of the
  canonical Saint John catalog.
- `https://data-muniorg.hub.arcgis.com` — 71/71 items already contained by the
  Alaska Geoportal.
- `https://trees.dc.gov` — thematic child catalog substantially contained by
  Open Data DC.
- `https://geohub-igua-sala-tecnica-sergipe-ii-tpfe.hub.arcgis.com` — exact
  duplicate of `https://geohubiguase-tpfe.hub.arcgis.com`.
- `https://od-dev2-ottawa.opendata.arcgis.com` and
  `https://arkansas-gis-hub-beta-agio.hub.arcgis.com` — development/beta hosts.
- `https://hubber-pets-unite-dcdev.hub.arcgis.com`,
  `https://huntington-botanical-gardens-esri-uc-2026-huntingtonbg.hub.arcgis.com`,
  and `https://esri-uc-2026-adopt-a-tree-government-admin.hub.arcgis.com` — demo
  or event sites rather than durable public catalogs.

## Watchlist

The remaining successfully harvested candidates stay in the local index for a
second-pass stability check but are not in `portals.toml`. Most are small
project/thematic Hub sites, have fewer than ten datasets, use a non-canonical
ArcGIS hostname, or need publisher verification. Promote them only after a
later incremental probe confirms that the domain, catalog scope, and publisher
remain stable.

## Second discovery tranche

A second ArcGIS sample (search results 501–1000 ordered by modification time)
found 161 scoped catalogs, 160 of them not yet in the local index, representing
about 14,650 advertised datasets. Twelve broad institutional catalogs were
full-harvested successfully; eleven were promoted after duplicate analysis. The
canonical Alaska domain (`https://gis.data.alaska.gov`) replaced the older Hub
hostname because it contains 841 of the old catalog's 851 records plus
additional records.

A sample of 10,000 records from the OpenDataSoft federation hub exposed 217
source domains: 169 answered the Explore API, 132 were new to the local index,
and together advertised about 75,500 datasets. Twelve geographically diverse
institutional portals were full-harvested and promoted. The very large Banque
de France and PNDB catalogs remain candidates pending cost and duplication
analysis.

## Future client findings

- DKAN does not require an immediate dedicated client: tested DKAN portals
  (Medicaid.gov and Dompu Satu Data) expose valid Project Open Data `data.json`
  catalogs and are now configured through `profile = "static_json"`.
- GeoNetwork / OGC catalogue records are a real uncovered family; tracked in
  [issue #177](https://github.com/AndreaBozzo/Ceres/issues/177).
- STAC is a real uncovered family, but should be indexed at Collection level by
  default to avoid creating one Ceres dataset per imagery scene; tracked in
  [issue #178](https://github.com/AndreaBozzo/Ceres/issues/178).

## Earlier-client coverage pass

The older client families now have dedicated parser fixtures for CKAN
`package_search`, udata JSON-LD with Hydra pagination, and SPARQL result JSON,
completing the fixture coverage alongside the newer clients.

Discovery candidates were compared against the local database before promotion,
including alternate frontend/backend hostnames. This rejected UK, Ireland,
Romania, Ukraine, and other already-harvested CKAN catalogs that were absent only
from the active TOML entries. Three genuinely new CKAN backends completed full
metadata-only harvests without failures: Ann Arbor (95 datasets), Northern
Ireland (1,092), and SSEN (35).

Five additional US agency `data.json` feeds were tested against the existing
Data.gov aggregate. FTC, DHS, Treasury, and NIST had 100% exact-title overlap and
were removed from both the curation set and local database. The National Archives
feed was retained on the watchlist: 84 records harvested without failures, with
34 titles absent from `catalog.data.gov`.

## CKAN ecosystem and linked-DCAT pass

The CKAN Ecosystem Catalog map feed exposed 682 registered sites. Direct API
probing found 408 live `package_search` endpoints after the initial database
hostname filter. A geographically varied twelve-portal tranche was then fully
harvested and checked against every title already in the local index.

Eight CKAN catalogs were retained after full metadata-only harvests with no
dataset failures:

- Ontario (2,951 records; 33.6% exact-title overlap)
- Northern Territory (1,072; 0.2%)
- Mauritius (849; 0.1%)
- Kyrgyzstan (1,405; 0.2%)
- NHS Business Services Authority (2,203; below 0.1%)
- Gifu Prefecture (1,881; 0.1%)
- City of Zurich (933; 73.3%, but about 249 locally unique titles)
- CIOOS Pacific (3,502; 1.6%)

The Parliament of the Canary Islands (100% overlap), Malaga (99.8%), and
Comunidad de Madrid (98.8%) were removed because `datos.gob.es` already covers
them. MapAction was also removed: its API advertises roughly 4,550 datasets but
the paged payload fails CKAN dataset decoding, so the catalog cannot currently
be harvested reliably.

No additional production udata REST portal survived the live probe beyond the
French, Luxembourgish, and Portuguese catalogs already present in the index.
SPARQL discovery was more productive: the Slovak national endpoint harvested
11,682 datasets without failures. Only 18.4% of its normalized titles were
already present, adding roughly 9,535 titles. Data Vlaanderen returned only
three of six declared DCAT resources and blocked partition queries with HTTP
403; the EU Publications Office endpoint exposed RDF application profiles
rather than harvestable open-data records. Both were rejected.

## OGC CSW implementation harvest pass

The new capability-driven CSW 2.0.2 client was exercised against independent
live catalogs before promotion. Five catalogs completed full metadata-only
harvests with no final record failures and preserved raw source XML:

- EMODnet central catalogue: 7,167 records processed (7,164 unique stored IDs)
- Copernicus Marine: 307 records
- Marine Regions / VLIZ: 831 records
- EEA marine SDI: 73 records
- French Géoplateforme: 314 records

The live session exposed two interoperability cases now covered by the client:
pretty-printed ISO XML with leading whitespace before `CharacterString` values,
and legacy ISO records without citation titles. The latter are retained using
their stable identifiers as display fallbacks. BGS was removed because its
documented GeoNetwork endpoint returned HTTP 404; Italy's RNDT exposed live
capabilities but rejected the client's ISO `GetRecords` query shape.

The same session added four reliable missing catalogs through existing clients:
Western Pennsylvania CKAN (369), U.S. Treasury `data.json` (233), Rouen
Métropole OpenDataSoft (107), and Arizona AZGeo ArcGIS Hub (703). Socrata probes
for Seattle and Austin returned zero dataset-scoped records and were rejected.
The EU Publications Office SPARQL endpoint advertised DCAT resources but yielded
zero records through Ceres' normalized metadata query, confirming the earlier
decision not to curate it.

Overall the session added 10,101 stored rows and nine logical portal sources,
raising the local index from 2,232,257 rows / 373 sources to 2,242,358 rows /
382 sources.
