INSERT INTO rules.ladder_member BY NAME
SELECT ladder_group_id, product_id, rank FROM build.ladder;
