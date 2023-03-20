SELECT toDate(demo_alias.pt_d) AS __fcol_50, sum(demo_alias.d_a_cnt) AS __fcol_52, sum(demo_alias.d_a_cnt) AS __fcol_54 FROM cbg.demo AS demo_alias GROUP BY toDate(demo_alias.pt_d)
