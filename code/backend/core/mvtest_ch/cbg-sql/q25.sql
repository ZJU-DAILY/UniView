SELECT demo_alias.d_a_cnt AS __fcol_15, demo_alias.rom AS __fcol_4, demo_alias.c_range AS __fcol_23, demo_alias.pt_d AS __fcol_45, multiIf(toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'bbb '), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb'), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb '), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb  ttt'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc'), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc '), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'iii'), 'nnn', toUInt8(demo_alias.d_p_name = 'ppp'), 'nnn', toUInt8(demo_alias.d_p_name = 'ppp'), 'nnn', toUInt8(demo_alias.d_p_name = 'kkk'), 'nnn', toUInt8(demo_alias.d_p_name = 'fff5'), 'nnn', toUInt8(demo_alias.d_p_name = 'fff5 hhh'), 'nnn', toUInt8(demo_alias.d_p_name = 'eee4'), 'vvv', toUInt8(demo_alias.d_p_name = 'eee'), 'vvv', toUInt8(demo_alias.d_p_name = 'hhh'), 'mmm', toUInt8(demo_alias.d_p_name = 'hhh '), 'mmm', toUInt8(demo_alias.d_p_name = 'ggg'), 'mmm', toUInt8(demo_alias.d_p_name = 'ggg '), 'mmm', toUInt8(demo_alias.d_p_name = 'hhh'), 'mmm', toUInt8(demo_alias.d_p_name = 'hh'), 'jj', toUInt8(demo_alias.d_p_name = 'hh'), 'jj', toUInt8(demo_alias.d_p_name = 'gg'), 'jj', toUInt8(demo_alias.d_p_name = ' mm'), 'mm', toUInt8(demo_alias.d_p_name = 'ff'), 'mm', toUInt8(demo_alias.d_p_name = 'ee'), 'mm', toUInt8(demo_alias.d_p_name = 'dd'), 'mm', toUInt8(demo_alias.d_p_name = 'cc'), 'mm', toUInt8(demo_alias.d_p_name = 'bb'), 'mm', toUInt8(demo_alias.d_p_name = 'aa'), 'aa', toUInt8(demo_alias.d_p_name = 'zzz'), 'aa', toUInt8(demo_alias.d_p_name = 'zzz'), 'aa', toUInt8(demo_alias.d_p_name = 'aa'), 'aa', toUInt8(demo_alias.d_p_name = 'sss'), 'aa', toUInt8(demo_alias.d_p_name = 'yyy'), 'kkk', toUInt8(demo_alias.d_p_name = 'yyy '), 'kkk', toUInt8(demo_alias.d_p_name = 'uuu'), 'lll', toUInt8(demo_alias.d_p_name = 'ccc'), 'lll', toUInt8(demo_alias.d_p_name = ' ccc'), 'ccc', toUInt8(demo_alias.d_p_name = ' ccc '), 'ccc', toUInt8(demo_alias.d_p_name = ' bbb'), 'bbb', toUInt8(demo_alias.d_p_name = 'www'), 'bbb', toUInt8(demo_alias.d_p_name = 'qqq'), 'bbb', toUInt8(demo_alias.d_p_name = 'qqq'), 'bbb', toUInt8(demo_alias.d_p_name = 'ccc'), 'nn', toUInt8(demo_alias.d_p_name = 'ccc'), 'nn', NULL) AS __fcol_49 FROM cbg.demo AS demo_alias WHERE ((multiIf(toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'bbb '), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb'), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb '), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb  ttt'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc'), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc '), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'iii'), 'nnn', toUInt8(demo_alias.d_p_name = 'ppp'), 'nnn', toUInt8(demo_alias.d_p_name = 'ppp'), 'nnn', toUInt8(demo_alias.d_p_name = 'kkk'), 'nnn', toUInt8(demo_alias.d_p_name = 'fff5'), 'nnn', toUInt8(demo_alias.d_p_name = 'fff5 hhh'), 'nnn', toUInt8(demo_alias.d_p_name = 'eee4'), 'vvv', toUInt8(demo_alias.d_p_name = 'eee'), 'vvv', toUInt8(demo_alias.d_p_name = 'hhh'), 'mmm', toUInt8(demo_alias.d_p_name = 'hhh '), 'mmm', toUInt8(demo_alias.d_p_name = 'ggg'), 'mmm', toUInt8(demo_alias.d_p_name = 'ggg '), 'mmm', toUInt8(demo_alias.d_p_name = 'hhh'), 'mmm', toUInt8(demo_alias.d_p_name = 'hh'), 'jj', toUInt8(demo_alias.d_p_name = 'hh'), 'jj', toUInt8(demo_alias.d_p_name = 'gg'), 'jj', toUInt8(demo_alias.d_p_name = ' mm'), 'mm', toUInt8(demo_alias.d_p_name = 'ff'), 'mm', toUInt8(demo_alias.d_p_name = 'ee'), 'mm', toUInt8(demo_alias.d_p_name = 'dd'), 'mm', toUInt8(demo_alias.d_p_name = 'cc'), 'mm', toUInt8(demo_alias.d_p_name = 'bb'), 'mm', toUInt8(demo_alias.d_p_name = 'aa'), 'aa', toUInt8(demo_alias.d_p_name = 'zzz'), 'aa', toUInt8(demo_alias.d_p_name = 'zzz'), 'aa', toUInt8(demo_alias.d_p_name = 'aa'), 'aa', toUInt8(demo_alias.d_p_name = 'sss'), 'aa', toUInt8(demo_alias.d_p_name = 'yyy'), 'kkk', toUInt8(demo_alias.d_p_name = 'yyy '), 'kkk', toUInt8(demo_alias.d_p_name = 'uuu'), 'lll', toUInt8(demo_alias.d_p_name = 'ccc'), 'lll', toUInt8(demo_alias.d_p_name = ' ccc'), 'ccc', toUInt8(demo_alias.d_p_name = ' ccc '), 'ccc', toUInt8(demo_alias.d_p_name = ' bbb'), 'bbb', toUInt8(demo_alias.d_p_name = 'www'), 'bbb', toUInt8(demo_alias.d_p_name = 'qqq'), 'bbb', toUInt8(demo_alias.d_p_name = 'qqq'), 'bbb', toUInt8(demo_alias.d_p_name = 'ccc'), 'nn', toUInt8(demo_alias.d_p_name = 'ccc'), 'nn', NULL) NOT IN ('')) AND isNotNull(multiIf(toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'bbb '), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb'), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb '), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb  ttt'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc'), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc '), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'iii'), 'nnn', toUInt8(demo_alias.d_p_name = 'ppp'), 'nnn', toUInt8(demo_alias.d_p_name = 'ppp'), 'nnn', toUInt8(demo_alias.d_p_name = 'kkk'), 'nnn', toUInt8(demo_alias.d_p_name = 'fff5'), 'nnn', toUInt8(demo_alias.d_p_name = 'fff5 hhh'), 'nnn', toUInt8(demo_alias.d_p_name = 'eee4'), 'vvv', toUInt8(demo_alias.d_p_name = 'eee'), 'vvv', toUInt8(demo_alias.d_p_name = 'hhh'), 'mmm', toUInt8(demo_alias.d_p_name = 'hhh '), 'mmm', toUInt8(demo_alias.d_p_name = 'ggg'), 'mmm', toUInt8(demo_alias.d_p_name = 'ggg '), 'mmm', toUInt8(demo_alias.d_p_name = 'hhh'), 'mmm', toUInt8(demo_alias.d_p_name = 'hh'), 'jj', toUInt8(demo_alias.d_p_name = 'hh'), 'jj', toUInt8(demo_alias.d_p_name = 'gg'), 'jj', toUInt8(demo_alias.d_p_name = ' mm'), 'mm', toUInt8(demo_alias.d_p_name = 'ff'), 'mm', toUInt8(demo_alias.d_p_name = 'ee'), 'mm', toUInt8(demo_alias.d_p_name = 'dd'), 'mm', toUInt8(demo_alias.d_p_name = 'cc'), 'mm', toUInt8(demo_alias.d_p_name = 'bb'), 'mm', toUInt8(demo_alias.d_p_name = 'aa'), 'aa', toUInt8(demo_alias.d_p_name = 'zzz'), 'aa', toUInt8(demo_alias.d_p_name = 'zzz'), 'aa', toUInt8(demo_alias.d_p_name = 'aa'), 'aa', toUInt8(demo_alias.d_p_name = 'sss'), 'aa', toUInt8(demo_alias.d_p_name = 'yyy'), 'kkk', toUInt8(demo_alias.d_p_name = 'yyy '), 'kkk', toUInt8(demo_alias.d_p_name = 'uuu'), 'lll', toUInt8(demo_alias.d_p_name = 'ccc'), 'lll', toUInt8(demo_alias.d_p_name = ' ccc'), 'ccc', toUInt8(demo_alias.d_p_name = ' ccc '), 'ccc', toUInt8(demo_alias.d_p_name = ' bbb'), 'bbb', toUInt8(demo_alias.d_p_name = 'www'), 'bbb', toUInt8(demo_alias.d_p_name = 'qqq'), 'bbb', toUInt8(demo_alias.d_p_name = 'qqq'), 'bbb', toUInt8(demo_alias.d_p_name = 'ccc'), 'nn', toUInt8(demo_alias.d_p_name = 'ccc'), 'nn', NULL))) AND (((((((((__fcol_23 NOT IN ('null')) OR (__fcol_23 = '') OR isNull(__fcol_23)) AND (__fcol_49 IN ('mmm', 'jj', 'mm', 'aa', 'kkk'))) AND (__fcol_4 IN ('128'))) AND (__fcol_4 IN ('128'))) AND (__fcol_45 < toDate(16356096000))) AND (__fcol_45 >= toDate(163552))) AND (__fcol_45 < toDate(16356096000))) AND (__fcol_45 >= toDate(163552)))
