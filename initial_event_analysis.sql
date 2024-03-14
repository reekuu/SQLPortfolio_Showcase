SELECT e.event_time, e.sequence_num, e.amount, d.dimension_type, d.grouping, d.identifier, grp.attribute
FROM (
  SELECT *, IF(@previous <> entity_id, @row_num:=1, @row_num:=@row_num+1) AS sequence_num, @previous:=entity_id 
  FROM events, (SELECT @row_num:=0) AS row_num, (SELECT @previous:='') AS previous
  ORDER BY entity_id ASC, event_time ASC
) AS e
JOIN dimensions AS d USING(entity_id)
LEFT JOIN groups grp USING(grouping)
WHERE sequence_num = 1
  AND grp.category IN ('category1', 'category2')
  AND d.entity_id NOT IN (SELECT entity_id FROM internal_entities);