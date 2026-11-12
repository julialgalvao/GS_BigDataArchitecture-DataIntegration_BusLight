-- 5. IMPACTO: quantos minutos de exposição no ponto os usuários deixam de passar
-- com o aviso "saia de casa às...". Considera 1 viagem por dia útil (22 por mês)
-- no ponto favorito e na faixa horária preferida de cada usuário.
WITH por_usuario AS (
    SELECT u.sk_usuario,
           u.telefone_mascarado,
           u.faixa_horaria_preferida,
           AVG(s.minutos_evitados) AS minutos_evitados_por_viagem
    FROM workspace.buslight.dim_usuario u
    JOIN workspace.buslight.vw_mvp_saia_de_casa s
      ON s.codigo_parada = u.sk_ponto_favorito
     AND s.faixa_horaria = u.faixa_horaria_preferida
    GROUP BY u.sk_usuario, u.telefone_mascarado, u.faixa_horaria_preferida
)
SELECT COUNT(*)                                          AS usuarios_beneficiados,
       ROUND(AVG(minutos_evitados_por_viagem), 1)        AS minutos_evitados_por_viagem,
       ROUND(AVG(minutos_evitados_por_viagem) * 22 / 60, 1) AS horas_evitadas_por_usuario_mes,
       ROUND(SUM(minutos_evitados_por_viagem) * 22 / 60, 0) AS horas_evitadas_total_mes,
       SUM(CASE WHEN faixa_horaria_preferida = 'noite' THEN 1 ELSE 0 END) AS usuarios_noturnos
FROM por_usuario;
