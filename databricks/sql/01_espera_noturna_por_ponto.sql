-- 1. Quanto tempo, em média, se espera em cada ponto à noite?
SELECT p.nome AS ponto,
       p.regiao,
       ROUND(AVG(f.espera_min), 1) AS espera_media_min,
       MAX(f.espera_min)           AS maior_espera_min,
       COUNT(*)                    AS amostras
FROM workspace.buslight.fato_espera_ponto f
JOIN workspace.buslight.dim_ponto p ON f.sk_ponto = p.sk_ponto
JOIN workspace.buslight.dim_tempo t ON f.sk_tempo = t.sk_tempo
WHERE t.faixa_horaria = 'noite'
GROUP BY p.nome, p.regiao
ORDER BY espera_media_min DESC;
