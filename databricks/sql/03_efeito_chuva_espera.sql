-- 3. Quanto a chuva aumenta a espera?
SELECT c.faixa_chuva,
       ROUND(AVG(f.espera_min), 1) AS espera_media_min,
       COUNT(*)                    AS amostras
FROM workspace.buslight.fato_espera_ponto f
JOIN workspace.buslight.dim_clima c ON f.sk_clima = c.sk_clima
GROUP BY c.faixa_chuva
ORDER BY espera_media_min DESC;
