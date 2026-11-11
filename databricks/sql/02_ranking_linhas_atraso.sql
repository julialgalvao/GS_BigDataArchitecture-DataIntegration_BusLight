-- 2. Quais linhas mais atrasam?
SELECT l.letreiro AS linha,
       l.terminal_principal || ' / ' || l.terminal_secundario AS trajeto,
       ROUND(AVG(GREATEST(f.atraso_min, 0)), 1) AS atraso_medio_min,
       ROUND(AVG(f.espera_min), 1)              AS espera_media_min,
       COUNT(*)                                 AS amostras
FROM workspace.buslight.fato_espera_ponto f
JOIN workspace.buslight.dim_linha l ON f.sk_linha = l.sk_linha
GROUP BY l.letreiro, l.terminal_principal, l.terminal_secundario
HAVING COUNT(*) >= 3
ORDER BY atraso_medio_min DESC
LIMIT 10;
