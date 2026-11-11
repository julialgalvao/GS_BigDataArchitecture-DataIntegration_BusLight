-- 4. Quais pontos são críticos? (espera noturna acima da média geral da noite)
WITH noite AS (
    SELECT f.sk_ponto, f.espera_min
    FROM workspace.buslight.fato_espera_ponto f
    JOIN workspace.buslight.dim_tempo t ON f.sk_tempo = t.sk_tempo
    WHERE t.faixa_horaria = 'noite'
)
SELECT p.nome AS ponto,
       p.endereco,
       ROUND(AVG(n.espera_min), 1) AS espera_noturna_min,
       ROUND((SELECT AVG(espera_min) FROM noite), 1) AS media_geral_noite_min
FROM noite n
JOIN workspace.buslight.dim_ponto p ON n.sk_ponto = p.sk_ponto
GROUP BY p.nome, p.endereco
HAVING AVG(n.espera_min) > (SELECT AVG(espera_min) FROM noite)
ORDER BY espera_noturna_min DESC;
