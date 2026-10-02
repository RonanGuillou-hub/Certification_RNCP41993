# Gouvernance des données et de l'IA : étude de cas Spotify

Livrables du **Bloc 1 (RNCP41993BC01)** de la certification **RNCP41993 Architecte IA** :
*Concevoir et piloter la gouvernance des données et des systèmes d'IA*.

| | |
|---|---|
| **Auteur** | Ronan GUILLOU |
| **Version** | 1.0 |
| **Date** | 30/09/2026 |
| **Cas d'étude** | Spotify |

> Projet réalisé à des fins pédagogiques, à partir d'un business case fourni dans le cadre de la formation. Il n'est pas affilié à Spotify et n'utilise aucune donnée interne de l'entreprise.

## Contexte

Spotify compte plus de 450 millions d'utilisateurs actifs dans plus de 180 pays. Ses données (écoute, métadonnées de contenu, facturation, marketing) sont cloisonnées par département, soumises au RGPD, au CCPA et à PCI-DSS, et alimentent un moteur de recommandation fondé sur le machine learning.

Le projet répond à ce contexte en trois temps : **diagnostiquer** la maturité des données, **définir** la politique et les rôles de gouvernance, puis **planifier** sa mise en œuvre, avec un pilote centré sur l'IA.

## Contenu du dépôt

```
.
├── README.md
└── docs/
    ├── D1_L1_Maturite_donnees.docx
    ├── D1_L2_Politique_gourvernance.docx
    ├── D1_L2_Organigramme.docx
    ├── D1_L3_Implementation_data_gouvernance.docx
    └── D1_Annexes.docx
```

| Livrable | Document | Contenu |
|---|---|---|
| **L1** | [Maturité des données](docs/D1_L1_Maturite_donnees.docx) | Diagnostic de maturité sur 9 piliers, avec une échelle de type Gartner à 5 niveaux. Justification de chaque note, défis clés de gouvernance et gouvernance de l'IA. |
| **L2** | [Politique de gouvernance](docs/D1_L2_Politique_gourvernance.docx) | Principes de gouvernance, conformité réglementaire (RGPD, CCPA/CPRA, PCI-DSS, AI Act), rôles et responsabilités, matrice RACI. |
| **L2** | [Organigramme](docs/D1_L2_Organigramme.docx) | Schéma de l'organisation cible : Direction générale, CDO, DPO, Data Governance Committee, Center of Excellence, Data Owners et Stewards, Model Owners et AI Risk Owner. |
| **L3** | [Plan d'implémentation](docs/D1_L3_Implementation_data_gouvernance.docx) | Modèle organisationnel, outils, pilote de 6 mois, KPI, risques, formation, déploiement et audits. |
| **Annexes** | [Glossaire](docs/D1_Annexes.docx) | Définitions des termes et acronymes utilisés dans les livrables (21 entrées). |

Ordre de lecture conseillé : L1, L2, L3, puis les annexes en référence.

## Résumé des livrables

### L1 : diagnostic de maturité

- Note moyenne de **2,1 sur 5** : Spotify est au niveau 2, une gouvernance encore **réactive**.
- Huit piliers sur neuf sont au niveau 2. Seule l'architecture atteint le niveau 3.
- Cible : 3,9 en moyenne à 18-24 mois (niveau 4 visé sur la plupart des piliers).
- Trois axes de travail : formaliser les procédures et les responsabilités, documenter les pratiques, former les équipes.

### L2 : politique et rôles

- Neuf principes organisés autour de **trois piliers** (qualité et interopérabilité, sécurité, conformité) et d'une **extension dédiée à l'IA**.
- Exigences réglementaires traduites en contrôles de gouvernance : RGPD, CCPA/CPRA, PCI-DSS, AI Act.
- Rôles définis : CDO, Data Governance Committee, DPO, Data Owners, Data Stewards, équipe sécurité et plateforme, Model Owner, AI Risk Owner.
- Une matrice RACI attribue un seul « A » (Approuve) par activité.

### L3 : plan d'implémentation

- **Modèle retenu :** Center of Excellence, justifié face aux modèles centralisé et embarqué.
- **Outils :** classés par domaine du DAMA-DMBOK (conformité, métadonnées, qualité, sécurité, intégration et lineage), avec une extension pour la gouvernance de l'IA.
- **Pilote :** 6 mois, sur les données d'entraînement du système de recommandation, avec équipe, calendrier, points de vérité et réunion mensuelle de suivi.
- **KPI :** indicateurs par but (qualité, accès, risques, conformité, adoption) et tableau de bord.
- **Risques :** 7 risques traités, dont la sécurité des accès, les biais et la conformité.
- **Formation et accessibilité :** formation par rôle avant le mois 3.
- **Déploiement :** extension par étapes sur 12 à 18 mois, audits (continu, conformité, indépendant) et veille réglementaire.

## Sources citées dans les livrables

- Business case Spotify (cas d'étude de la formation)
- Governance Principles Guide
- Executive Q&A Guide
- Tech Tools Overview
- Checklist de conformité (14 exigences RGPD, CCPA, PCI-DSS)
- Référentiel DAMA-DMBOK

## Statut

Livrables de la version 1.0, remis pour l'évaluation du Bloc 1. Les chiffres du cas d'étude (utilisateurs, pays) datent de 2023 et proviennent du business case.