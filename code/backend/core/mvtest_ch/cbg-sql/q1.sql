SELECT multiIf(demo_alias.u_seg IN ('1', '2', '3', '4', '5', '6'), '6', demo_alias.u_seg IN ('10', '11', '12', '', '8', '9'), '1', demo_alias.u_seg IN ('13', '14', '15', '16', '1', '18'), '1', demo_alias.u_seg IN ('19', '', '21', '22', '23', '24'), '2', demo_alias.u_seg IN ('25', '26', '2', '28', '29', ''), '2', demo_alias.u_seg IN ('31', '32', '33', '34', '35', '36'), '3', demo_alias.u_seg IN ('36'), '3', toString(demo_alias.u_seg)) AS __fcol_50
FROM cbg.demo AS demo_alias
WHERE (demo_alias.pt_d >= toDate(163552))
        AND (demo_alias.pt_d < toDate(16356096000))
        AND (demo_alias.pt_d >= toDate(163552))
        AND (demo_alias.pt_d < toDate(16356096000))
        AND (multiIf(toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'bbb '), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb'), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb '), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb ttt'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc'), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc '), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'iii'), 'nnn', toUInt8(demo_alias.d_p_name = 'ppp'), 'nnn', toUInt8(demo_alias.d_p_name = 'ppp'), 'nnn', toUInt8(demo_alias.d_p_name = 'kkk'), 'nnn', toUInt8(demo_alias.d_p_name = 'fff5'), 'nnn', toUInt8(demo_alias.d_p_name = 'fff5 hhh'), 'nnn', toUInt8(demo_alias.d_p_name = 'eee4'), 'vvv', toUInt8(demo_alias.d_p_name = 'eee'), 'vvv', toUInt8(demo_alias.d_p_name = 'hhh'), 'mmm', toUInt8(demo_alias.d_p_name = 'hhh '), 'mmm', toUInt8(demo_alias.d_p_name = 'ggg'), 'mmm', toUInt8(demo_alias.d_p_name = 'ggg '), 'mmm', toUInt8(demo_alias.d_p_name = 'hhh'), 'mmm', toUInt8(demo_alias.d_p_name = 'hh'), 'jj', toUInt8(demo_alias.d_p_name = 'hh'), 'jj', toUInt8(demo_alias.d_p_name = 'gg'), 'jj', toUInt8(demo_alias.d_p_name = ' mm'), 'mm', toUInt8(demo_alias.d_p_name = 'ff'), 'mm', toUInt8(demo_alias.d_p_name = 'ee'), 'mm', toUInt8(demo_alias.d_p_name = 'dd'), 'mm', toUInt8(demo_alias.d_p_name = 'cc'), 'mm', toUInt8(demo_alias.d_p_name = 'bb'), 'mm', toUInt8(demo_alias.d_p_name = 'aa'), 'aa', toUInt8(demo_alias.d_p_name = 'zzz'), 'aa', toUInt8(demo_alias.d_p_name = 'zzz'), 'aa', toUInt8(demo_alias.d_p_name = 'aa'), 'aa', toUInt8(demo_alias.d_p_name = 'sss'), 'aa', toUInt8(demo_alias.d_p_name = 'yyy'), 'kkk', toUInt8(demo_alias.d_p_name = 'yyy '), 'kkk', toUInt8(demo_alias.d_p_name = 'uuu'), 'lll', toUInt8(demo_alias.d_p_name = 'ccc'), 'lll', toUInt8(demo_alias.d_p_name = ' ccc'), 'ccc', toUInt8(demo_alias.d_p_name = ' ccc '), 'ccc', toUInt8(demo_alias.d_p_name = ' bbb'), 'bbb', toUInt8(demo_alias.d_p_name = 'www'), 'bbb', toUInt8(demo_alias.d_p_name = 'qqq'), 'bbb', toUInt8(demo_alias.d_p_name = 'qqq'), 'bbb', toUInt8(demo_alias.d_p_name = 'ccc'), 'nn', toUInt8(demo_alias.d_p_name = 'ccc'), 'nn', NULL) NOT IN (''))
        AND isNotNull(multiIf(toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'aaa'), 'nn', toUInt8(demo_alias.d_p_name = 'bbb '), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb'), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb '), 'ttt', toUInt8(demo_alias.d_p_name = 'bbb ttt'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc'), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc '), 'ttt', toUInt8(demo_alias.d_p_name = 'ccc'), 'ttt', toUInt8(demo_alias.d_p_name = 'ddd'), 'ttt', toUInt8(demo_alias.d_p_name = 'iii'), 'nnn', toUInt8(demo_alias.d_p_name = 'ppp'), 'nnn', toUInt8(demo_alias.d_p_name = 'ppp'), 'nnn', toUInt8(demo_alias.d_p_name = 'kkk'), 'nnn', toUInt8(demo_alias.d_p_name = 'fff5'), 'nnn', toUInt8(demo_alias.d_p_name = 'fff5 hhh'), 'nnn', toUInt8(demo_alias.d_p_name = 'eee4'), 'vvv', toUInt8(demo_alias.d_p_name = 'eee'), 'vvv', toUInt8(demo_alias.d_p_name = 'hhh'), 'mmm', toUInt8(demo_alias.d_p_name = 'hhh '), 'mmm', toUInt8(demo_alias.d_p_name = 'ggg'), 'mmm', toUInt8(demo_alias.d_p_name = 'ggg '), 'mmm', toUInt8(demo_alias.d_p_name = 'hhh'), 'mmm', toUInt8(demo_alias.d_p_name = 'hh'), 'jj', toUInt8(demo_alias.d_p_name = 'hh'), 'jj', toUInt8(demo_alias.d_p_name = 'gg'), 'jj', toUInt8(demo_alias.d_p_name = ' mm'), 'mm', toUInt8(demo_alias.d_p_name = 'ff'), 'mm', toUInt8(demo_alias.d_p_name = 'ee'), 'mm', toUInt8(demo_alias.d_p_name = 'dd'), 'mm', toUInt8(demo_alias.d_p_name = 'cc'), 'mm', toUInt8(demo_alias.d_p_name = 'bb'), 'mm', toUInt8(demo_alias.d_p_name = 'aa'), 'aa', toUInt8(demo_alias.d_p_name = 'zzz'), 'aa', toUInt8(demo_alias.d_p_name = 'zzz'), 'aa', toUInt8(demo_alias.d_p_name = 'aa'), 'aa', toUInt8(demo_alias.d_p_name = 'sss'), 'aa', toUInt8(demo_alias.d_p_name = 'yyy'), 'kkk', toUInt8(demo_alias.d_p_name = 'yyy '), 'kkk', toUInt8(demo_alias.d_p_name = 'uuu'), 'lll', toUInt8(demo_alias.d_p_name = 'ccc'), 'lll', toUInt8(demo_alias.d_p_name = ' ccc'), 'ccc', toUInt8(demo_alias.d_p_name = ' ccc '), 'ccc', toUInt8(demo_alias.d_p_name = ' bbb'), 'bbb', toUInt8(demo_alias.d_p_name = 'www'), 'bbb', toUInt8(demo_alias.d_p_name = 'qqq'), 'bbb', toUInt8(demo_alias.d_p_name = 'qqq'), 'bbb', toUInt8(demo_alias.d_p_name = 'ccc'), 'nn', toUInt8(demo_alias.d_p_name = 'ccc'), 'nn', NULL))
GROUP BY  demo_alias.u_seg, multiIf(demo_alias.u_seg IN ('1', '2', '3', '4', '5', '6'), '', demo_alias.u_seg IN ('10', '11', '12', '', '8', '9'), '1', demo_alias.u_seg IN ('13', '14', '15', '16', '1', '18'), '1', demo_alias.u_seg IN ('19', '', '21', '22', '23', '24'), '2', demo_alias.u_seg IN ('25', '26', '2', '28', '29', ''), '2', demo_alias.u_seg IN ('31', '32', '33', '34', '35', '36'), '3', demo_alias.u_seg IN ('36'), '3', toString(demo_alias.u_seg)) 