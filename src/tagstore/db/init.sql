-- # PERFORMANCE TUNING

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS tag_label_like_idx ON tag USING GIN (label gin_trgm_ops);
CREATE INDEX IF NOT EXISTS actor_label_like_idx ON actor USING GIN (label gin_trgm_ops);
SET pg_trgm.similarity_threshold=0.3;

-- # MATERIALIZED VIEWS

CREATE MATERIALIZED VIEW IF NOT EXISTS statistics AS
    SELECT
        explicit.network,
        nr_tags,
        no_labels as nr_labels,
        explicit.nr_identifiers as nr_identifiers_explicit,
        COALESCE(implicit.nr_identifiers, explicit.nr_identifiers) as nr_identifiers_implicit
    FROM
        (SELECT
            network,
            COUNT(*) as nr_tags,
            COUNT(DISTINCT label) AS no_labels,
            COUNT(DISTINCT identifier) AS nr_identifiers
         FROM
            tag
         GROUP BY
            network
        ) explicit
    LEFT JOIN
        (SELECT
            SUM(gs_cluster_no_addr) AS nr_identifiers,
            network
         FROM
            (SELECT DISTINCT ON (gs_cluster_id, network)
                network,
                gs_cluster_no_addr
             FROM address_cluster_mapping
            ) t
         GROUP
            BY network
        ) implicit
    ON implicit.network = explicit.network;

CREATE UNIQUE INDEX IF NOT EXISTS statistics_by_network
  ON statistics (network);

CREATE MATERIALIZED VIEW IF NOT EXISTS  tag_count_by_cluster AS
    SELECT
        t.network,
        acm.gs_cluster_id,
        tp.acl_group,
        count(t.identifier) as count
    FROM
        tag t,
        tagpack tp,
        address_cluster_mapping acm
    WHERE
        acm.address=t.identifier
        AND acm.network=t.network
        AND t.tagpack=tp.id
    GROUP BY
        t.network,
        acm.gs_cluster_id,
        tp.acl_group;

CREATE UNIQUE INDEX IF NOT EXISTS tag_count_curr_cluster_index ON tag_count_by_cluster (network, gs_cluster_id, acl_group);

/* In the end this view fulfils the following requirements in junction with
 * REST's `list_entity_tags_by_entity`:
 *  If there is no address tag with is_cluster_definer = True -> no cluster tag
 *  If there is an address tag with is_cluster_definer = True -> assign on cluster level
 *  If there are several address tags with is_cluster_definer = True -> take the one with higher confidence value
 *  If there are several address tags with is_cluster_definer = True and same confidence value and if the labels are the same -> take one of them and assign it to cluster level
 *  If cluster size = 1 and there is an address tag on that single address -> assign to cluster level
 *  If cluster size = 1 and there are several address tags on that single address -> assign the one with highest confidence
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS best_cluster_tag AS
    SELECT
        acm.gs_cluster_id as cluster_id,
        t.network as network,
        t.id as tag_id
    FROM
        tag t,
        address_cluster_mapping acm,
        confidence c,
        tagpack tp
    WHERE
        acm.address=t.identifier
        AND acm.network=t.network
        AND t.is_cluster_definer=true
        AND t.confidence=c.id
        AND tp.id=t.tagpack
    GROUP BY
        c.level,
        t.id,
        -- t.category,
        t.network,
        acm.gs_cluster_id,
        tp.acl_group
    UNION
        SELECT
            acm.gs_cluster_id as cluster_id,
            t.network as network,
            t.id as tag_id
        FROM
            address_cluster_mapping acm,
            tag t,
            confidence c,
            tagpack tp
        WHERE
            c.id=t.confidence
            and tp.id=t.tagpack
            and t.identifier=acm.address
            and t.network=acm.network
            and acm.gs_cluster_no_addr = 1
        GROUP BY
            t.id,
            -- t.category,
            t.network,
            acm.gs_cluster_id,
            tp.acl_group
        HAVING
            every(t.is_cluster_definer=false or t.is_cluster_definer is null);

CREATE INDEX IF NOT EXISTS cluster_tags_by_clstr ON best_cluster_tag (cluster_id);
CREATE INDEX IF NOT EXISTS cluster_tags_by_clstr_and_network ON best_cluster_tag (network, cluster_id);
CREATE UNIQUE INDEX IF NOT EXISTS cluster_tag_unique ON best_cluster_tag (network, cluster_id, tag_id);

-- Quality measures

DROP TABLE IF EXISTS address_quality;
CREATE TABLE IF NOT EXISTS address_quality(
	id SERIAL PRIMARY KEY,
	network VARCHAR,
	identifier VARCHAR,
	n_tags INTEGER,
	n_dif_tags INTEGER,
	total_pairs INTEGER,
	q1 INTEGER,
	q2 INTEGER,
	q3 INTEGER,
	q4 INTEGER,
	quality NUMERIC
);

-- Procedure to calculate the quality measures, usage: CALL calculate_quality();

CREATE OR REPLACE PROCEDURE calculate_quality(actor BOOLEAN DEFAULT FALSE)
LANGUAGE PLPGSQL
AS $$
DECLARE
	i RECORD;
	e RECORD;
	s RECORD;
	sim NUMERIC;
	_tag_column TEXT;
BEGIN
	DROP TABLE IF EXISTS quality_pairs;
	CREATE TEMP TABLE IF NOT EXISTS quality_pairs(
		id SERIAL PRIMARY KEY,
		network VARCHAR,
		identifier VARCHAR,
		label1 VARCHAR,
		label2 VARCHAR,
		sim NUMERIC
	);
	DROP TABLE IF EXISTS quality_labels;
	CREATE TEMP TABLE IF NOT EXISTS quality_labels(
		id SERIAL PRIMARY KEY,
		network VARCHAR,
		identifier VARCHAR,
		label VARCHAR,
		label_id INTEGER
	);
	IF actor THEN _tag_column='actor'; ELSE _tag_column='label'; END IF;
	FOR i in EXECUTE ( format(
		'SELECT t.network, t.identifier, COUNT(DISTINCT t.%1$I) n_labels
		FROM tag t
		GROUP BY network, identifier
		HAVING COUNT(DISTINCT t.%1$I) > 1'
		, _tag_column
	))
	LOOP
		FOR e in SELECT * FROM tag WHERE network=i.network AND identifier=i.identifier LOOP
			-- RAISE NOTICE '%:%', e.identifier, e.label;
			FOR s in SELECT u.label label, similarity(u.label, e.label) simi FROM quality_labels u WHERE u.identifier = e.identifier LOOP
				-- RAISE NOTICE '% <-> % = %', e.label, s.label, s.simi;
				sim = s.simi;
				INSERT INTO quality_pairs (network, identifier, label1, label2, sim)
				VALUES (e.network, e.identifier, e.label, s.label, sim);
			END LOOP;
		        INSERT INTO quality_labels (network, identifier, label, label_id)
		        VALUES (e.network, e.identifier, e.label, e.id);
		END LOOP;
	END LOOP;
END $$;

-- Save quality measures into address_quality table

CREATE OR REPLACE PROCEDURE insert_address_quality()
LANGUAGE PLPGSQL
AS $$
BEGIN
TRUNCATE address_quality;
INSERT INTO address_quality
	(network, identifier, n_tags, n_dif_tags, total_pairs, q1, q2, q3, q4, quality)
SELECT
	tags.network, tags.identifier, tags.n_tags, tags.n_dif_tags,
	pairs.total total_pairs, sim.q1, sim.q2, sim.q3, sim.q4,
	1-((sim.q1*0.25+sim.q2*0.5+sim.q3*0.75+sim.q4*1.0)/pairs.total::float) quality
FROM (
	SELECT
		t.network, t.identifier, COUNT(t.label) n_tags, COUNT(DISTINCT(t.label)) n_dif_tags
	FROM tag t
	GROUP BY t.network, t.identifier
	HAVING COUNT(DISTINCT(t.label)) > 1
) tags
LEFT OUTER JOIN (
	SELECT
		q.network, q.identifier, COUNT(q.sim) n_sim
	FROM quality_pairs q
	WHERE q.sim <= 0.25
	GROUP BY q.network, q.identifier
) quality_q1
ON tags.network = quality_q1.network AND tags.identifier = quality_q1.identifier
LEFT OUTER JOIN (
	SELECT
		q.network, q.identifier, COUNT(q.sim) n_sim
	FROM quality_pairs q
	WHERE q.sim > 0.25 AND q.sim <= 0.5
	GROUP BY q.network, q.identifier
) quality_q2
ON tags.network = quality_q2.network AND tags.identifier = quality_q2.identifier
LEFT OUTER JOIN (
	SELECT
		q.network, q.identifier, COUNT(q.sim) n_sim
	FROM quality_pairs q
	WHERE q.sim > 0.50 AND q.sim <= 0.75
	GROUP BY q.network, q.identifier
) quality_q3
ON tags.network = quality_q3.network AND tags.identifier = quality_q3.identifier
LEFT OUTER JOIN (
	SELECT
		q.network, q.identifier, COUNT(q.sim) n_sim
	FROM quality_pairs q
	WHERE q.sim > 0.75
	GROUP BY q.network, q.identifier
) quality_q4
ON tags.network = quality_q4.network AND tags.identifier = quality_q4.identifier
CROSS JOIN LATERAL (
	SELECT
		coalesce(quality_q1.n_sim, 0),
		coalesce(quality_q2.n_sim, 0),
		coalesce(quality_q3.n_sim, 0),
		coalesce(quality_q4.n_sim, 0)
) as sim(q1, q2, q3, q4)
CROSS JOIN LATERAL (
	SELECT
		(sim.q1+sim.q2+sim.q3+sim.q4)
) as pairs(total);
END $$;
