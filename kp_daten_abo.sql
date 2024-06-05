############## FINAL QUERY #############



WITH end_date AS (
    SELECT uid, org_name, MAX(subb.end_dtz) max_end_dtz
    FROM `fmg-ulm-data-hub-prd.aggregation.person`, UNNEST(sub) subb
    WHERE uid = master_uid AND org_name NOT IN ("fmg_mpro", "fmg_pz")
    GROUP BY uid, org_name
),

abo_typ AS (
    SELECT dest.uid, dest.org_name, 
        MIN(CASE WHEN medium = 'print' THEN '1'
                 WHEN medium = 'ep' THEN '2'
                 WHEN medium = 'pc' THEN '3'
                 ELSE '9' END) AS AboTyp
    FROM `fmg-ulm-data-hub-prd.aggregation.person` dest, UNNEST(sub) s
    LEFT JOIN end_date
        ON (dest.uid = end_date.uid AND dest.org_name = end_date.org_name AND s.end_dtz = end_date.max_end_dtz)
    WHERE end_date.max_end_dtz IS NOT NULL AND dest.uid = dest.master_uid AND dest.org_name NOT IN ("fmg_mpro", "fmg_pz")
    GROUP BY dest.uid, org_name
),

address AS (
    SELECT uid, org_name, postal_address.residential_unit.street AS Strasse, postal_address.residential_unit.house_number AS Hausnummer,
        postal_address.residential_unit.postcode AS PLZ, postal_address.residential_unit.town AS Stadt, postal_address.residential_unit.country AS Land, 
        postal_address.recipient.first_name AS Vorname, postal_address.recipient.last_name AS Name,
        postal_address.recipient.salutation AS Anrede_GP
    FROM `fmg-ulm-data-hub-prd.aggregation.person`
    WHERE uid = master_uid AND org_name NOT IN ("fmg_mpro", "fmg_pz") AND postal_address.residential_unit.id IS NOT NULL AND postal_address.recipient.company IS NULL
),

KundinSeitMedium AS (
    SELECT dest.uid, dest.org_name, 
        MIN(s.start_dtz) AS Kundin_Seit_Medium
    FROM `fmg-ulm-data-hub-prd.aggregation.person` dest, UNNEST(sub) s
    LEFT JOIN abo_typ
        ON (dest.uid = abo_typ.uid AND dest.org_name = abo_typ.org_name AND s.medium = CASE
                  WHEN abo_typ.AboTyp = '1' THEN 'print'
                  WHEN abo_typ.AboTyp = '2' THEN 'ep'
                  WHEN abo_typ.AboTyp = '3' THEN 'pc'
                  ELSE '9' END)
    WHERE abo_typ.AboTyp IS NOT NULL AND dest.uid = dest.master_uid AND dest.org_name NOT IN ("fmg_mpro", "fmg_pz")
    GROUP BY dest.uid, dest.org_name
),

KundinSeitFunke AS (
    SELECT dest.uid, dest.org_name, MIN(s.start_dtz) AS Kundin_Seit_Funke
    FROM `fmg-ulm-data-hub-prd.aggregation.person` dest, UNNEST(sub) s     
    WHERE dest.uid = dest.master_uid AND dest.org_name NOT IN ("fmg_mpro", "fmg_pz")
    GROUP BY dest.uid, dest.org_name
),

main_KuPro AS (
    SELECT dest.uid AS UID, dest.org_name AS Org_Name, MAX(s.title) AS Titel, s.medium AS Medium, MAX(s.role) AS Role,
        CASE WHEN CAST(max_end_dtz AS DATE) > CURRENT_DATE THEN 'aktiv' ELSE 'inaktiv' END AS Status,
        PLZ, Stadt, Land, Anrede_GP, Vorname, Name, Strasse, Hausnummer, Kundin_Seit_Medium, Kundin_Seit_Funke
    FROM `fmg-ulm-data-hub-prd.aggregation.person` dest, UNNEST(sub) s
    LEFT JOIN end_date
        ON (dest.uid = end_date.uid AND dest.org_name = end_date.org_name AND s.end_dtz = end_date.max_end_dtz)
    LEFT JOIN abo_typ
        ON (dest.uid = abo_typ.uid AND dest.org_name = abo_typ.org_name)
    LEFT JOIN address
        ON (dest.uid = address.uid AND dest.org_name = address.org_name)
    LEFT JOIN KundinSeitMedium
        ON (dest.uid = KundinSeitMedium.uid AND dest.org_name = KundinSeitMedium.org_name)
    LEFT JOIN KundinSeitFunke
        ON (dest.uid = KundinSeitFunke.uid AND dest.org_name = KundinSeitFunke.org_name)
    WHERE end_date.max_end_dtz IS NOT NULL AND dest.uid = dest.master_uid AND dest.org_name NOT IN ("fmg_mpro", "fmg_pz")
        AND (CASE WHEN s.medium = 'print' THEN '1'
                  WHEN s.medium = 'ep' THEN '2'
                  WHEN s.medium = 'pc' THEN '3'
                  ELSE '9' END) = abo_typ.AboTyp
    GROUP BY dest.uid, dest.org_name, s.medium, max_end_dtz, Strasse, Hausnummer, PLZ, Stadt, Land, Anrede_GP, Vorname, Name, Kundin_Seit_Medium, Kundin_Seit_Funke
),

GeschlechtAlter AS (
    SELECT 
      bvg.Vorname AS Vorname,
      bvg.Geburtsjahr AS Geburtsjahr,
      EXTRACT(YEAR FROM CURRENT_DATE) - bvg.Geburtsjahr AS Alter,
      bvgg.Geschlecht AS Gender
    FROM `fmg-dcx-data-science.Basistabellen.BAS_Vornamen_Geburtsjahr` bvg
    JOIN `fmg-dcx-data-science.Basistabellen.BAS_Vornamen_Geschlecht` bvgg
    ON LOWER(TRIM(bvg.Vorname)) = LOWER(TRIM(bvgg.Vorname))
    WHERE bvgg.Geschlecht IN ('weiblich', 'männlich')
),
PLZ_to_KGS AS (
    SELECT
        PLZ,
        KGS,
        KGS5,
        KGS3
    FROM `fmg-dcx-data-science.b4p.STA_Postleitzahl_zu_KGS`)

SELECT 
  CAST(kp.UID as STRING) AS UID, 
  kp.Org_Name, 
  kp.Titel, 
  kp.Medium, 
  kp.Role, 
  kp.Status, 
  kp.Anrede_GP, 
  kp.Vorname, 
  kp.Name, 
  kp.Strasse, 
  kp.Hausnummer, 
  kp.PLZ, 
  kp.Stadt, 
  kp.Land,
  kp.Kundin_Seit_Medium, 
  kp.Kundin_Seit_Funke,
  CASE
    WHEN kp.Titel IN ('bhn', 'bsz', 'bz', 'bgz', 'hk', 'bwz', 'bzv', 'bgr', 'bwn') THEN 'Niedersachsen'
    WHEN kp.Titel IN ('ikz', 'wp', 'nrz', 'wr', 'wn', 'waz') THEN 'NRW'
    WHEN kp.Titel IN ('otz', 'tlz', 'ta') THEN 'Thüringen'
    WHEN kp.Titel = 'ha' THEN 'Hamburg'
    WHEN kp.Titel = 'bm' THEN 'Berlin'
    ELSE NULL
  END AS NameTageszeitung,
  CASE
    WHEN kp.Titel IN ('bhn', 'bsz', 'bz', 'bgz', 'hk', 'bwz', 'bzv', 'bgr', 'bwn') THEN 'FUNKE_Medien_NS_Braunschw'
    WHEN kp.Titel IN ('ikz', 'wp', 'nrz', 'wr', 'wn', 'waz') THEN 'FUNKE_Medien_NRW'
    WHEN kp.Titel IN ('otz', 'tlz', 'ta') THEN 'FUNKE_Medien_Thüringen'
    WHEN kp.Titel = 'ha' THEN 'FUNKE_Medien_Hamb_Hamburger_Abendblatt'
    WHEN kp.Titel = 'bm' THEN 'FUNKE_Medien_Berlin_BerlinerMorgenpost'
    ELSE NULL
  END AS NameTageszeitungB4P,
  ga.Gender, 
  ga.Alter,
  plz_kgs.KGS,
  plz_kgs.KGS5,
  plz_kgs.KGS3,
  CASE
    WHEN ga.Alter < 30 THEN 'Unter 30'
    WHEN ga.Alter >= 30 AND ga.Alter < 40 THEN '30-40'
    WHEN ga.Alter >= 40 AND ga.Alter < 50 THEN '40-50'
    WHEN ga.Alter >= 50 AND ga.Alter < 60 THEN '50-60'
    WHEN ga.Alter >= 60 AND ga.Alter < 70 THEN '60-70'
    WHEN ga.Alter >= 70 AND ga.Alter < 80 THEN '70-80'
    WHEN ga.Alter >= 80 THEN 'Über 80'
    ELSE NULL
  END AS Altersklasse_b4p
FROM main_KuPro kp
LEFT JOIN GeschlechtAlter ga
  ON LOWER(TRIM(kp.Vorname)) = LOWER(TRIM(ga.Vorname))
LEFT JOIN PLZ_to_KGS plz_kgs
  ON kp.PLZ = plz_kgs.PLZ
WHERE kp.Titel NOT IN ('kind', 'rs');




############## OLD QUERY ##############

with end_date as (
      select uid, org_name, max(subb.end_dtz) max_end_dtz
      from `fmg-ulm-data-hub-prd.aggregation.person`, unnest(sub) subb
      where uid = master_uid 
      AND org_name NOT IN ("fmg_mpro", "fmg_pz")
      group by uid, org_name  
      ), ## gucken wegen enddatum könnte ein issue sein; 2099n oder 9999

abo_typ as (
      select dest.uid, dest.org_name, 
        min(case 
          when medium = 'print' then '1' 
          when medium = 'ep' then '2' 
          when medium = 'pc' then '3' 
          else '9' 
        end) as AboTyp
      from `fmg-ulm-data-hub-prd.aggregation.person` dest, unnest(sub) s
      left join end_date
      on (dest.uid = end_date.uid and dest.org_name = end_date.org_name and s.end_dtz = end_date.max_end_dtz)
      where end_date.max_end_dtz is not null
      and dest.uid = dest.master_uid
      and dest.org_name NOT IN ("fmg_mpro", "fmg_pz")
      group by dest.uid, org_name
      order by dest.uid
      ), ## passt schon so, weil pro medium das maximale enddatum sonst das wollen wir nicht! 

-- Adresse zum Datensatz
address as (
      select uid, org_name, postal_address.residential_unit.street as Strasse, postal_address.residential_unit.house_number as Hausnummer,
        postal_address.residential_unit.postcode as PLZ, postal_address.residential_unit.town as Stadt, postal_address.residential_unit.country as Land, 
        postal_address.recipient.first_name Vorname, postal_address.recipient.last_name Name, 
        postal_address.recipient.salutation as Anrede_GP
      from `fmg-ulm-data-hub-prd.aggregation.person`
      where uid = master_uid
      and org_name NOT IN ("fmg_mpro", "fmg_pz")
      and postal_address.residential_unit.id is not NULL
      and postal_address.recipient.company is NULL
      ),

-- Kundin_seit_Medium: wann wurde das erste Abo des aktuellen Mediums / Abotyps abgeschlossen, unabhängig von ggf. längeren Unterbrechungen
KundinSeitMedium as (
      select dest.uid, dest.org_name, 
        min(s.start_dtz) as Kundin_Seit_Medium
      from `fmg-ulm-data-hub-prd.aggregation.person` dest, unnest(sub) s
      left join abo_typ
      on (dest.uid = abo_typ.uid 
        and dest.org_name = abo_typ.org_name 
        and s.medium = case 
                          when abo_typ.AboTyp = '1' then 'print'
                          when abo_typ.AboTyp = '2' then 'ep' 
                          when abo_typ.AboTyp = '3' then 'pc' 
                          else '9' 
                        end)
      where abo_typ.AboTyp is not null
      and dest.uid = dest.master_uid
      and dest.org_name NOT IN ("fmg_mpro", "fmg_pz")
      group by dest.uid, dest.org_name
      ),

-- Kundin_seit_Funke: wann wurde das erste Abo bei Funke abgeschlossen, unabhängig vom Medium und ggf. längeren Unterbrechungen
KundinSeitFunke as (
      select dest.uid, dest.org_name, min(s.start_dtz) as Kundin_Seit_Funke
      from `fmg-ulm-data-hub-prd.aggregation.person` dest, unnest(sub) s     
      where dest.uid = dest.master_uid
      and dest.org_name NOT IN ("fmg_mpro", "fmg_pz")
      group by dest.uid, dest.org_name
      ),

main_KuPro as (
      select dest.uid as UID, dest.org_name as Org_Name, max(s.title) as Titel, s.medium as Medium, max(s.role) as Role,
        case when cast(max_end_dtz as date) > current_date then 'aktiv' else 'inaktiv' end as Status,
        PLZ, Stadt, Land, Anrede_GP, Vorname, Name, Strasse, Hausnummer, Kundin_Seit_Medium, Kundin_Seit_Funke
      from `fmg-ulm-data-hub-prd.aggregation.person` dest, unnest(sub) s
      left join end_date
      on (dest.uid = end_date.uid and dest.org_name = end_date.org_name and s.end_dtz = end_date.max_end_dtz)
      left join abo_typ
      on (dest.uid = abo_typ.uid and dest.org_name = abo_typ.org_name)
      left join address 
      on (dest.uid = address.uid and dest.org_name = address.org_name)
      left join KundinSeitMedium 
      on (dest.uid = KundinSeitMedium.uid and dest.org_name = KundinSeitMedium.org_name)
      left join KundinSeitFunke 
      on (dest.uid = KundinSeitFunke.uid and dest.org_name = KundinSeitFunke.org_name)
      where end_date.max_end_dtz is not null
      and dest.uid = dest.master_uid 
      and dest.org_name NOT IN ("fmg_mpro", "fmg_pz")
      and (case 
            when s.medium = 'print' then '1' 
            when s.medium = 'ep' then '2' 
            when s.medium = 'pc' then '3' 
            else '9' 
          end) = abo_typ.AboTyp
      group by dest.uid, dest.org_name, s.medium, max_end_dtz, Strasse, Hausnummer, PLZ, Stadt, Land, Anrede_GP, Vorname, Name, 
      Kundin_Seit_Medium, Kundin_Seit_Funke
      order by dest.uid)
select 
  cast(UID as String) UID, Org_Name, Titel, Medium, Role, Status, Anrede_GP, Vorname, Name, Strasse, Hausnummer, PLZ, Stadt, Land,
  Kundin_Seit_Medium, Kundin_Seit_Funke,
  CASE
    WHEN Titel IN ('bhn', 'bsz', 'bz', 'bgz', 'hk', 'bwz', 'bzv', 'bgr', 'bwn') THEN 'Niedersachsen'
    WHEN Titel IN ('ikz', 'wp', 'nrz', 'wr', 'wn', 'waz') THEN 'NRW'
    WHEN Titel IN ('otz', 'tlz', 'ta') THEN 'Thüringen'
    WHEN Titel = 'ha' THEN 'Hamburg'
    WHEN Titel = 'bm' THEN 'Berlin'
  END AS NameTageszeitung,
  CASE
    WHEN Titel IN ('bhn', 'bsz', 'bz', 'bgz', 'hk', 'bwz', 'bzv', 'bgr', 'bwn') THEN 'FUNKE_Medien_NS_Braunschw'
    WHEN Titel IN ('ikz', 'wp', 'nrz', 'wr', 'wn', 'waz') THEN 'FUNKE_Medien_NRW'
    WHEN Titel IN ('otz', 'tlz', 'ta') THEN 'FUNKE_Medien_Thüringen'
    WHEN Titel = 'ha' THEN 'FUNKE_Medien_Hamb_Hamburger_Abendblatt'
    WHEN Titel = 'bm' THEN 'FUNKE_Medien_Berlin_BerlinerMorgenpost'
  END AS NameTageszeitungB4P
from main_KuPro
WHERE Titel NOT IN ('kind', 'rs')

## inhaltlich gucken; dass das current date < end date heißt nicht, dass der Kunde aktiv ist; Start Datum muss auch noch < current date!!! case when noch mit rein!
## oben bei max date oben nochmal checken
## schwierigkeit wieder auf gleiches select zu kommen, with beilassen
## beim nächsten mal ohne with, lieber exist
## wenn datum y tage in der zukunft liegt, dann nehm alle, dann nicht nur die die zufällig vom lieferanten höher bewertet wurden
## aus nachhaltigkeitssicht sind not ins nicht zu empfehlen; whitelisting ist immer besser als blacklisting, weil meine sonst neue parameter immer direkt mit reinbekommt, die neu reinkommen; lieber selbst entscheiden was ich haben möchte! könnte passieren, dass man falsche daten selektiert und damit dann auch aussteuert!


############ Tabelle KGS zu PLZ ################

SELECT
*
FROM
  `fmg-dcx-data-science.b4p.STA_Postleitzahl_zu_KGS`
LIMIT
  1000




################## TESTS ###############

WITH MediaPriority AS (
    SELECT
        master_uid,
        FIRST_VALUE(medium) OVER (PARTITION BY master_uid ORDER BY
            CASE medium
                WHEN 'print' THEN 1
                WHEN 'ep' THEN 2
                WHEN 'pc' THEN 3
            END) AS Highest_Medium,
        FIRST_VALUE(start_dtz) OVER (PARTITION BY master_uid ORDER BY
            CASE medium
                WHEN 'print' THEN 1
                WHEN 'ep' THEN 2
                WHEN 'pc' THEN 3
            END) AS Start_Date,
        FIRST_VALUE(end_dtz) OVER (PARTITION BY master_uid ORDER BY
            CASE medium
                WHEN 'print' THEN 1
                WHEN 'ep' THEN 2
                WHEN 'pc' THEN 3
            END) AS End_Date
    FROM `fmg-ulm-data-hub-prd.aggregation.person`
    JOIN UNNEST(sub) AS s
    WHERE org_name NOT IN ("fmg_mpro", "fmg_pz")
        AND postal_address.recipient.salutation IN ('Herr', 'Frau')
),
ReferencePriority AS (
    SELECT
        master_uid,
        FIRST_VALUE(r.address) OVER (PARTITION BY master_uid ORDER BY
            CASE r.reference
                WHEN 'email' THEN 1
                ELSE 2
            END) AS Email_Address,
        FIRST_VALUE(r.reference) OVER (PARTITION BY master_uid ORDER BY
            CASE r.reference
                WHEN 'email' THEN 1
                ELSE 2
            END) AS Reference_Type
    FROM `fmg-ulm-data-hub-prd.aggregation.person`
    JOIN UNNEST(reference) AS r
    WHERE org_name NOT IN ("fmg_mpro", "fmg_pz")
        AND postal_address.recipient.salutation IN ('Herr', 'Frau', 'Undefiniert')
)
SELECT DISTINCT
    p.master_uid,
    p.org_name,
    m.Highest_Medium AS Sub_Medium,
    m.Start_Date,
    m.End_Date,
    r.Email_Address AS Reference_Address,
    r.Reference_Type,
    p.postal_address.residential_unit.postcode AS PLZ,
    p.postal_address.recipient.salutation AS Anrede,
    p.postal_address.recipient.first_name as First_Name,
    p.postal_address.residential_unit.country as Country,
    CASE
        WHEN p.postal_address.recipient.salutation = 'Herr' THEN 'männlich'
        ELSE 'weiblich'
    END AS Geschlecht
FROM `fmg-ulm-data-hub-prd.aggregation.person` p
JOIN MediaPriority m ON p.master_uid = m.master_uid
JOIN ReferencePriority r ON p.master_uid = r.master_uid
WHERE p.org_name NOT IN ("fmg_mpro", "fmg_pz")
AND p.postal_address.recipient.salutation IN ('Herr', 'Frau')


#########################################################
############### Kundenprofil ############################
#########################################################

with
-- Finde aus den Aufträgen das maximale Enddatum von Aufträgen, um den relevantesten Auftrag identifizieren zu können.
-- Glücklicher Weise ist das Datum für unbegrenzte PC-Aufträge 2099-12-31 und für Print / ePaper 9999-12-30, da Print / ePaper auch eine höhere
-- Wertigkeit zugeordnet bekommen sollen
end_date as (
      select uid, org_name, max(subb.end_dtz) max_end_dtz
      from `fmg-ulm-data-hub-prd.aggregation.person` , unnest(sub) subb
      where uid = master_uid 
      group by uid, org_name  
      ),

abo_typ as (
      select dest.uid, dest.org_name, 
        min(case 
          when medium = 'print' then '1' 
          when medium = 'ep' then '2' 
          when medium = 'pc' then '3' 
          else '9' 
        end) as AboTyp
      from `fmg-ulm-data-hub-prd.aggregation.person` dest, unnest(sub) s
      left join end_date
      on (dest.uid = end_date.uid and dest.org_name = end_date.org_name and s.end_dtz = end_date.max_end_dtz)
      where end_date.max_end_dtz is not null
      and dest.uid = dest.master_uid
      --and dest.uid = 100008674
      group by dest.uid, dest.org_name
      order by dest.uid
      ),

-- Adresse zum Datensatz
address as (
      select uid, org_name, postal_address.residential_unit.street as Strasse, postal_address.residential_unit.house_number as Hausnummer,
        postal_address.residential_unit.postcode as PLZ, postal_address.residential_unit.town as Stadt, postal_address.residential_unit.country as Land, 
        postal_address.recipient.first_name Vorname, postal_address.recipient.last_name Name, 
        postal_address.recipient.salutation as Anrede_GP
      from `fmg-ulm-data-hub-prd.aggregation.person`
      where uid = master_uid
      and postal_address.residential_unit.id is not NULL
      and postal_address.recipient.company is NULL
      ),

-- Kundin_seit_Medium: wann wurde das erste Abo des aktuellen Mediums / Abotyps abgeschlossen, unabhängig von ggf. längeren Unterbrechungen
KundinSeitMedium as (
      select dest.uid, dest.org_name, 
        min(s.start_dtz) as Kundin_Seit_Medium
      from `fmg-ulm-data-hub-prd.aggregation.person` dest, unnest(sub) s
      left join abo_typ
      on (dest.uid = abo_typ.uid 
        and dest.org_name = abo_typ.org_name 
        and s.medium = case 
                          when abo_typ.AboTyp = '1' then 'print'
                          when abo_typ.AboTyp = '2' then 'ep' 
                          when abo_typ.AboTyp = '3' then 'pc' 
                          else '9' 
                        end)
      where abo_typ.AboTyp is not null
      and dest.uid = dest.master_uid
      group by dest.uid, dest.org_name
      ),

-- Kundin_seit_Funke: wann wurde das erste Abo bei Funke abgeschlossen, unabhängig vom Medium und ggf. längeren Unterbrechungen
KundinSeitFunke as (
      select dest.uid, dest.org_name, min(s.start_dtz) as Kundin_Seit_Funke
      from `fmg-ulm-data-hub-prd.aggregation.person` dest, unnest(sub) s     
      where dest.uid = dest.master_uid
      group by dest.uid, dest.org_name
      ),
      
main_KuPro as 
      (select dest.uid as UID, dest.org_name as Org_Name, max(s.title) as Titel, s.medium as Medium, max(s.role) as Role
        , case when cast(max_end_dtz as date) > current_date then 'aktiv' else 'inaktiv' end as Status
        , PLZ, Stadt, Land, Anrede_GP, Vorname, Name, Strasse, Hausnummer, Kundin_Seit_Medium, Kundin_Seit_Funke, Letzte_Nutzung_PC, Letzte_Nutzung_EP
      from `fmg-ulm-data-hub-prd.aggregation.person` dest, unnest(sub) s
      left join end_date
      on (dest.uid = end_date.uid and dest.org_name = end_date.org_name and s.end_dtz = end_date.max_end_dtz)
      left join abo_typ
      on (dest.uid = abo_typ.uid and dest.org_name = abo_typ.org_name)
      left join address 
      on (dest.uid = address.uid and dest.org_name = address.org_name)
      left join KundinSeitMedium 
      on (dest.uid = KundinSeitMedium.uid and dest.org_name = KundinSeitMedium.org_name)
      left join KundinSeitFunke 
      on (dest.uid = KundinSeitFunke.uid and dest.org_name = KundinSeitFunke.org_name)
      left join lastUsagePC 
      on (dest.uid = lastUsagePC.uid and dest.org_name = lastUsagePC.org_name)
      left join lastUsageEP 
      on (dest.uid = lastUsageEP.uid and dest.org_name = lastUsageEP.org_name)
      where end_date.max_end_dtz is not null
      and dest.uid = dest.master_uid 
      and (case 
            when s.medium = 'print' then '1' 
            when s.medium = 'ep' then '2' 
            when s.medium = 'pc' then '3' 
            else '9' 
          end) = abo_typ.AboTyp
      --and dest.uid = 100008674
      group by dest.uid, dest.org_name, s.medium, max_end_dtz, Strasse, Hausnummer, PLZ, Stadt, Land, Anrede_GP, Vorname, Name, 
      Kundin_Seit_Medium, Kundin_Seit_Funke, Letzte_Nutzung_PC, Letzte_Nutzung_EP
      order by dest.uid)






  ####################################################
  ####### ROUTINE AUS CUSTOMER SATISFYER #############


  BEGIN

-- Leere Delta-Tabelle
truncate table `fmg-dcx-surveys.Customer_Satisfyer_2022_Pers.HBAX_Customer_Satisfyer_Pers_Delta`;

insert into `fmg-dcx-surveys.Customer_Satisfyer_2022_Pers.HBAX_Customer_Satisfyer_Pers_Delta`
(UID,Org_Name, Titel, Medium, Role, Status, Anrede_GP, Vorname, Name, Strasse, Hausnummer, PLZ, Stadt, Land
, Kundin_Seit_Medium, Kundin_Seit_Funke, Letzte_Nutzung_PC, Letzte_Nutzung_EP
-- CX
,GP_Nummer,NPS, NPS_POS_ZUS, NPS_NEG_ZUS,SEX, `ALTER`, KINDER_HH_01, KINDER_HH_02, KINDER_HH_03, KINDER_HH_04, KINDER_HH_99
, BEW_GES, BEW_ASP_01, BEW_ASP_06, BEW_ASP_08, BEW_ASP_09, BEW_ASP_10, BEW_ASP_11, BEW_ASP_12, BEW_ASP_04, BEW_ASP_14
, ABO_VERL, ABO_VERL_ZUS, KANAL_WUNSCH_01, KANAL_WUNSCH_02, KANAL_WUNSCH_03, KANAL_WUNSCH_04, KANAL_WUNSCH_05
, KANAL_WUNSCH_06,KANAL_WUNSCH_07, KANAL_WUNSCH_08, KANAL_WUNSCH_99, KANAL_WUNSCH_ZUS)

-- Berechne Input und matche CX mit Kundenprofil
with
-- Finde aus den Aufträgen das maximale Enddatum von Aufträgen, um den relevantesten Auftrag identifizieren zu können.
-- Glücklicher Weise ist das Datum für unbegrenzte PC-Aufträge 2099-12-31 und für Print / ePaper 9999-12-30, da Print / ePaper auch eine höhere
-- Wertigkeit zugeordnet bekommen sollen
end_date as (
      select uid, org_name, max(subb.end_dtz) max_end_dtz
      from `fmg-ulm-data-hub-prd.aggregation.person` , unnest(sub) subb
      where uid = master_uid 
      group by uid, org_name  
      ),

-- Bestimme zum Abo mit der maximalen Laufzeit das wertigste Medium
-- Wertigkeit: Print wie Premium-Abo am höchsten, dann ep mit Digital Komplett und am niedrigsten pc PaidContent Only
abo_typ as (
      select dest.uid, dest.org_name, 
        min(case 
          when medium = 'print' then '1' 
          when medium = 'ep' then '2' 
          when medium = 'pc' then '3' 
          else '9' 
        end) as AboTyp
      from `fmg-ulm-data-hub-prd.aggregation.person` dest, unnest(sub) s
      left join end_date
      on (dest.uid = end_date.uid and dest.org_name = end_date.org_name and s.end_dtz = end_date.max_end_dtz)
      where end_date.max_end_dtz is not null
      and dest.uid = dest.master_uid
      --and dest.uid = 100008674
      group by dest.uid, dest.org_name
      order by dest.uid
      ),

-- Adresse zum Datensatz
address as (
      select uid, org_name, postal_address.residential_unit.street as Strasse, postal_address.residential_unit.house_number as Hausnummer,
        postal_address.residential_unit.postcode as PLZ, postal_address.residential_unit.town as Stadt, postal_address.residential_unit.country as Land, 
        postal_address.recipient.first_name Vorname, postal_address.recipient.last_name Name, 
        postal_address.recipient.salutation as Anrede_GP
      from `fmg-ulm-data-hub-prd.aggregation.person`
      where uid = master_uid
      and postal_address.residential_unit.id is not NULL
      and postal_address.recipient.company is NULL
      ),

-- Kundin_seit_Medium: wann wurde das erste Abo des aktuellen Mediums / Abotyps abgeschlossen, unabhängig von ggf. längeren Unterbrechungen
KundinSeitMedium as (
      select dest.uid, dest.org_name, 
        min(s.start_dtz) as Kundin_Seit_Medium
      from `fmg-ulm-data-hub-prd.aggregation.person` dest, unnest(sub) s
      left join abo_typ
      on (dest.uid = abo_typ.uid 
        and dest.org_name = abo_typ.org_name 
        and s.medium = case 
                          when abo_typ.AboTyp = '1' then 'print'
                          when abo_typ.AboTyp = '2' then 'ep' 
                          when abo_typ.AboTyp = '3' then 'pc' 
                          else '9' 
                        end)
      where abo_typ.AboTyp is not null
      and dest.uid = dest.master_uid
      group by dest.uid, dest.org_name
      ),

-- Kundin_seit_Funke: wann wurde das erste Abo bei Funke abgeschlossen, unabhängig vom Medium und ggf. längeren Unterbrechungen
KundinSeitFunke as (
      select dest.uid, dest.org_name, min(s.start_dtz) as Kundin_Seit_Funke
      from `fmg-ulm-data-hub-prd.aggregation.person` dest, unnest(sub) s     
      where dest.uid = dest.master_uid
      group by dest.uid, dest.org_name
      ),

-- Datum der letzten Nutzung für Paid Content
lastUsagePC as (
      SELECT uid, org_name, max(pu.last_usage) Letzte_Nutzung_PC
      FROM `fmg-ulm-data-hub-prd.aggregation.person` , unnest(product_usage) pu
      where pu.medium = 'pc'
      group by uid, org_name),

-- Datum der letzten Nutzung für E-Paper
lastUsageEP as (
      SELECT uid, org_name, max(pu.last_usage) Letzte_Nutzung_EP
      FROM `fmg-ulm-data-hub-prd.aggregation.person` , unnest(product_usage) pu
      where pu.medium = 'ep'
      group by uid, org_name),


-- CX-Survey Informationen
-- Wichtig ist hier das Mapping mit der GP-Nummer aus dem Kundenprofil, um von der GP-Nummer auf die zugehörige master_uid aus dem 
-- Kundenprofil zugreifen zu können. Die uid wird später genutzt, um weitere Informationen aus dem Kundenprofil in der Zieltabelle 
-- ergänzen zu können. 
CX as (
      select NPS, NPS_POS_ZUS, NPS_NEG_ZUS,SEX, `ALTER`, KINDER_HH_01, KINDER_HH_02, KINDER_HH_03, KINDER_HH_04, KINDER_HH_99
            , BEW_GES, BEW_ASP_01, BEW_ASP_06, BEW_ASP_08, BEW_ASP_09, BEW_ASP_10, BEW_ASP_11, BEW_ASP_12, BEW_ASP_04, BEW_ASP_14
            , ABO_VERL, ABO_VERL_ZUS, KANAL_WUNSCH_01, KANAL_WUNSCH_02, KANAL_WUNSCH_03, KANAL_WUNSCH_04, KANAL_WUNSCH_05
            , KANAL_WUNSCH_06,KANAL_WUNSCH_07, KANAL_WUNSCH_08, KANAL_WUNSCH_99, KANAL_WUNSCH_ZUS
            , case when Kundennr = '' then '' else right(concat('00',replace(Kundennr,' ','')),10) end as GP_Nummer, uid, org_name
      from `fmg-dcx-surveys.EXT_Lamapoll.CSAT_EP_V0A` dest
      left join (select uid, org_name, ref.address GP_Nummer
                 from `fmg-ulm-data-hub-prd.aggregation.person` , unnest(reference) ref
                 where ref.system = 'sap_msd'
                 and ref.reference = 'gp_nummer'
                 and master_uid = uid
                 group by uid, org_name, ref.address) sour
      on (right(concat('00',replace(Kundennr,' ','')),10) = (right(concat('00',sour.GP_Nummer),10)))
      --where Kundennr <> ''
      ),



-- Bestimme zum Record mit der maximalen Laufzeit und dem hochwertigsten Medium die übrigen relevanten Ausprägungen
-- Prüfen: was ist wenn jemand Premium und Digital Komplett hat???
main_KuPro as 
      (select dest.uid as UID, dest.org_name as Org_Name, max(s.title) as Titel, s.medium as Medium, max(s.role) as Role
        , case when cast(max_end_dtz as date) > current_date then 'aktiv' else 'inaktiv' end as Status
        , PLZ, Stadt, Land, Anrede_GP, Vorname, Name, Strasse, Hausnummer, Kundin_Seit_Medium, Kundin_Seit_Funke, Letzte_Nutzung_PC, Letzte_Nutzung_EP
      from `fmg-ulm-data-hub-prd.aggregation.person` dest, unnest(sub) s
      left join end_date
      on (dest.uid = end_date.uid and dest.org_name = end_date.org_name and s.end_dtz = end_date.max_end_dtz)
      left join abo_typ
      on (dest.uid = abo_typ.uid and dest.org_name = abo_typ.org_name)
      left join address 
      on (dest.uid = address.uid and dest.org_name = address.org_name)
      left join KundinSeitMedium 
      on (dest.uid = KundinSeitMedium.uid and dest.org_name = KundinSeitMedium.org_name)
      left join KundinSeitFunke 
      on (dest.uid = KundinSeitFunke.uid and dest.org_name = KundinSeitFunke.org_name)
      left join lastUsagePC 
      on (dest.uid = lastUsagePC.uid and dest.org_name = lastUsagePC.org_name)
      left join lastUsageEP 
      on (dest.uid = lastUsageEP.uid and dest.org_name = lastUsageEP.org_name)
      where end_date.max_end_dtz is not null
      and dest.uid = dest.master_uid 
      and (case 
            when s.medium = 'print' then '1' 
            when s.medium = 'ep' then '2' 
            when s.medium = 'pc' then '3' 
            else '9' 
          end) = abo_typ.AboTyp
      --and dest.uid = 100008674
      group by dest.uid, dest.org_name, s.medium, max_end_dtz, Strasse, Hausnummer, PLZ, Stadt, Land, Anrede_GP, Vorname, Name, 
      Kundin_Seit_Medium, Kundin_Seit_Funke, Letzte_Nutzung_PC, Letzte_Nutzung_EP
      order by dest.uid)



-- Ergebnis

select 
-- Kundenprofil
cast(sour.UID as String) UID, sour.Org_Name, Titel, Medium, Role, Status, Anrede_GP, Vorname, Name, Strasse, Hausnummer, PLZ, Stadt, Land
, Kundin_Seit_Medium, Kundin_Seit_Funke, Letzte_Nutzung_PC, Letzte_Nutzung_EP
-- CX
,GP_Nummer,cast(NPS as String) as NPS, NPS_POS_ZUS, NPS_NEG_ZUS, cast(SEX as String) as SEX, cast(`ALTER` as string) ALTER, cast(KINDER_HH_01 as String), cast(KINDER_HH_02 as String), cast(KINDER_HH_03 as String), cast(KINDER_HH_04 as String), cast(KINDER_HH_99 as String)
, cast(BEW_GES as String), cast(BEW_ASP_01 as String), cast(BEW_ASP_06 as String), cast(BEW_ASP_08 as String), cast(BEW_ASP_09 as String) 
, cast(BEW_ASP_10 as String), cast(BEW_ASP_11 as String), cast(BEW_ASP_12 as String), cast(BEW_ASP_04 as String), cast(BEW_ASP_14 as String)
, cast(ABO_VERL as String), cast(ABO_VERL_ZUS as String), cast(KANAL_WUNSCH_01 as String), cast(KANAL_WUNSCH_02 as String), cast(KANAL_WUNSCH_03 as String), cast(KANAL_WUNSCH_04 as String), cast(KANAL_WUNSCH_05 as String)
, cast(KANAL_WUNSCH_06 as String),cast(KANAL_WUNSCH_07 as String), cast(KANAL_WUNSCH_08 as String), cast(KANAL_WUNSCH_99 as String), cast(KANAL_WUNSCH_ZUS as String)
from CX dest
left join main_KuPro sour
on (dest.uid = sour.UID and dest.org_name = sour.org_name);











-- Übersetzung/Erzeugung übriger Codes

-- Update ALTERSKLASSE und ALTERSKLASSE_ID  
UPDATE `fmg-dcx-surveys.Customer_Satisfyer_2022_Pers.HBAX_Customer_Satisfyer_Pers_Delta` AS dest
set dest.ALTERSKLASSE = sour.AK_SURVEYS
FROM `fmg-dcx-surveys.Stammdaten.WX_ALTERSKLASSEN` AS sour 
WHERE dest.ALTER = sour.ALTER;

/*
-- Update STANDORT, STANDORT_ID, TITEL_ID 
UPDATE `fmg-dcx-surveys.Customer_Satisfyer_2022.HREP_Customer_Satisfyer_Basis_Delta` AS dest
set dest.STANDORT = sour.STANDORT_KURZ, dest.STANDORT_ORDER = sour.STANDORT_ORDER, dest.TITEL_ORDER = sour.TITEL_ORDER 
FROM `fmg-dcx-surveys.Stammdaten.WX_TITEL` AS sour 
WHERE dest.TITEL = sour.TITEL_SURVEY;

-- Update MEDIUM_ID
UPDATE `fmg-dcx-surveys.Customer_Satisfyer_2022.HREP_Customer_Satisfyer_Basis_Delta` AS dest
set dest.MEDIUM_ORDER = sour.MEDIUM_ORDER
FROM `fmg-dcx-surveys.Stammdaten.WX_MEDIUM` AS sour 
WHERE dest.MEDIUM = sour.MEDIUM_ID;

-- Update Kampagne und Kanal
CALL `fmg-dcx-surveys.Stammdaten.ZPRO_Update_Kampagne_Kanal`('`fmg-dcx-surveys.Customer_Satisfyer_2022.HREP_Customer_Satisfyer_Basis_Delta`');


-- Ergänze Geo-Informationen
update `fmg-dcx-surveys.Customer_Satisfyer_2022.HREP_Customer_Satisfyer_Basis_Delta` dest
set dest.GEO_KOORDINATE = sour.Punkt
from `fmg-dcx-surveys.Stammdaten.WX_GEO_ORT_FAV` sour
where (dest.ORT_FAV = sour.ORT_FAV)
and sour.ort_Fav is not null;
*/

-- Backup und Übertrage das Ergebnis in die Haupttabelle
/*truncate table `fmg-dcx-surveys.Customer_Satisfyer_2022_Pers.HBAX_Customer_Satisfyer_Pers_Backup`;

insert into `fmg-dcx-surveys.Customer_Satisfyer_2022_Pers.HBAX_Customer_Satisfyer_Pers_Backup`
select * from `fmg-dcx-surveys.Customer_Satisfyer_2022_Pers.BAX_Customer_Satisfyer_Pers`;

truncate table `fmg-dcx-surveys.Customer_Satisfyer_2022_Pers.BAX_Customer_Satisfyer_Pers`;

insert into `fmg-dcx-surveys.Customer_Satisfyer_2022_Pers.BAX_Customer_Satisfyer_Pers`
select * from `fmg-dcx-surveys.Customer_Satisfyer_2022_Pers.HBAX_Customer_Satisfyer_Pers_Delta`;

*/

/*
-- Übertrag des Ergebnisses in die Benchmark-Tabelle
truncate table  `fmg-dcx-surveys.Customer_Satisfyer_2022.REP_Customer_Satisfyer_Basis_B`;

insert into `fmg-dcx-surveys.Customer_Satisfyer_2022.REP_Customer_Satisfyer_Basis_B` 
SELECT * FROM `fmg-dcx-surveys.Customer_Satisfyer_2022.REP_Customer_Satisfyer_Basis`; 
*/ 

END