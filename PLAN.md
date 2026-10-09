# Claude Code : créer `GromNaN/mercure-mongodb`

## Objectif

Créer le dépôt GitHub public `GromNaN/mercure-mongodb`, puis implémenter un transport MongoDB pour le hub Mercure open source et fournir une documentation d’utilisation complète dans un README soigné, avec un schéma d’architecture clair.

Le transport doit permettre d’utiliser MongoDB comme seul service de stockage partagé : historique durable des mises à jour et synchronisation des instances Mercure. Ne pas réimplémenter le protocole Mercure, son endpoint HTTP, son authentification JWT ou son serveur SSE.

Hypothèses par défaut : dépôt public et README en anglais pour un projet open source international. Le README peut s’inspirer de la présentation éditoriale de Mercure.rocks, mais doit rester original et ne pas reprendre son logo, ses illustrations, ses textes ou son identité de marque. Aucun site GitHub Pages n’est à créer. Le nom de module Go visé est `github.com/GromNaN/mercure-mongodb`, à vérifier avant de l’inscrire dans les fichiers.

## Étape 0 — Créer le dépôt GitHub d’abord

1. Vérifier le dossier courant, la présence de `gh` et l’état d’authentification GitHub. Ne pas écraser ni réinitialiser un dossier ou dépôt existant.
2. Vérifier si `GromNaN/mercure-mongodb` existe déjà. S’il existe, le cloner ou travailler dessus sans écraser son contenu ; examiner les branches, fichiers et issues avant d’agir.
3. S’il n’existe pas et que `gh` est authentifié, créer le dépôt public `GromNaN/mercure-mongodb`, le cloner localement et configurer `origin`. Si la création distante est bloquée par l’absence d’authentification ou de permissions, s’arrêter et donner à l’utilisateur les étapes exactes pour créer le dépôt, puis reprendre dans le checkout fourni. Ne pas prétendre qu’un dépôt a été créé si ce n’est pas le cas.
4. Ajouter un `.gitignore` et une licence à choisir après examen des exigences du hub Mercure et des obligations AGPL. Ne pas choisir une licence incompatible ni annoncer une licence avant vérification.
5. Faire un premier commit propre qui établit la structure du projet avant de commencer l’implémentation. Ne jamais committer de secrets.

## Étape 1 — Définir le module et inspecter Mercure

Ce dépôt doit contenir un module de transport séparé, et non une copie ou un fork du code du hub. Avant le code fonctionnel :

* Vérifier la disponibilité et la propriété du nom de dépôt et du module Go ; inspecter tout contenu déjà présent.
* Cloner temporairement ou inspecter la version/tag de Mercure choisie comme cible. Relever la version de Go, Caddy et Mercure, l’interface Transport réelle, le mécanisme d’enregistrement/configuration des transports Caddy, le transport Bolt et les tests concernés.
* Lire les chemins d’ajout d’abonné, de publication, de fermeture et de rejeu historique. Comprendre précisément `Last-Event-ID` : ID personnalisé, absent, répété, inconnu, historique expiré, ordre des événements et frontière entre historique et live.
* Vérifier les conventions utilisées par le hub pour construire un binaire Caddy personnalisé et charger un module externe. Le résultat visé est un module compilé dans un binaire avec Mercure, pas un plugin Go chargé dynamiquement après compilation.
* Comparer les conventions avec les sources de départ, sans remplacer l’inspection du code correspondant au tag retenu : [interface Transport](https://github.com/dunglas/mercure/blob/main/transport.go), [package Mercure](https://pkg.go.dev/github.com/dunglas/mercure), [exemple de transport Redis/Caddy](https://github.com/desk-ly/mercure-transport-redis), [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changestreams/).
* Avant de coder, fournir un bref rapport avec le tag cible, les signatures exactes, la stratégie de compilation, les risques de compatibilité et la licence. Ne pas inventer des API ou directives de configuration.

## Étape 2 — ADR d’architecture avant l’implémentation

Ajouter un ADR au dépôt et y fixer les décisions suivantes. Si un point critique n’est pas résolu, s’arrêter à l’ADR et le signaler plutôt que d’implémenter une sémantique supposée.

1. Modèle d’événement MongoDB append-only : Update Mercure sérialisé, version de schéma, date d’écriture et curseur interne adapté au rejeu. Séparer si nécessaire l’ID Mercure visible du curseur interne.
2. Ordonnancement des écritures concurrentes : ordre global ou par partition/topic ; comment il est garanti avec plusieurs publishers ; coût et contention éventuels d’un compteur global. Ne pas traiter un timestamp ou un ObjectID comme un ordre total sans le démontrer.
3. Rejeu et rétention : index, fenêtre retenue, politique de nettoyage et réponse à un curseur expiré. Le contrat annoncé doit correspondre aux données réellement conservées.
4. Change Streams : flux de notifications entre les hubs et persistance des resume tokens/checkpoints. Définir une resynchronisation à partir de la collection durable quand un token a disparu de l’oplog.
5. Garanties après crash : préciser les cas de doublon et de perte entre lecture du Change Stream, diffusion aux abonnés en mémoire et sauvegarde du checkpoint. Ne pas promettre exactly-once sans preuve.
6. Multi-instance : chaque hub garde ses connexions SSE et abonnements actifs localement ; tous consomment le flux MongoDB et diffusent localement les mises à jour correspondantes. Traiter le doublon potentiel entre une publication locale et son événement Change Stream.
7. Cohérence rejeu/live : définir comment l’ajout d’un abonné établit un snapshot/cursor, rejoue l’historique, puis passe au live sans trou ni course.
8. Dépendance MongoDB : exiger un replica set ou un cluster shardé compatible Change Streams. MongoDB standalone n’est pas une cible valide pour les tests d’intégration Change Streams.

## Étape 3 — Squelette de dépôt et intégration Caddy

Créer une structure Go minimale adaptée aux conventions trouvées dans Mercure, avec le code du transport, les tests et le README principal à la racine. Ne pas créer de sous-répertoire `site/` ni d’application web. Ajuster après inspection plutôt que suivre aveuglément un nommage.

* Déclarer une configuration Caddy/Mercure dédiée au transport MongoDB, suivant le format réellement utilisé par la version ciblée.
* Configurer `go.mod` pour les versions compatibles du hub/Caddy et du pilote MongoDB officiel. Épingler les dépendances compatibles et documenter la matrice de versions supportées.
* Ajouter un exemple de build de binaire personnalisé avec Mercure et le module, suivant l’outil du projet (par exemple `xcaddy` si confirmé).
* Ajouter les premières vérifications CI : formatage, tests Go, analyse statique si le projet l’utilise, et build du module.
* Vérifier AGPL et compatibilité de licence avant de fixer le fichier LICENSE ou de distribuer un binaire combiné.

## Étape 4 — Implémenter le transport MongoDB

* Implémenter exactement l’interface transport de la version épinglée et enregistrer le module auprès de Caddy comme l’exige cette version.
* Exposer une configuration validée au démarrage : URI MongoDB via variable d’environnement, base, collection, rétention et timeouts nécessaires. Ne jamais écrire les credentials ou le contenu privé des événements dans les logs.
* Au provisionnement, établir la connexion, vérifier la compatibilité Change Streams, les permissions et les index ; fournir des erreurs opérables plutôt que continuer avec un mode dégradé silencieux.
* À la publication, persister l’événement durablement avant de le déclarer disponible, puis le rendre observable par les autres hubs via Change Streams.
* Pour le rejeu, lire l’historique depuis MongoDB selon le contrat Mercure observé. Respecter les règles d’autorisation/topic et le comportement des IDs de l’implémentation cible.
* Pour l’abonnement, respecter le contrat exact de l’interface, y compris le signal indiquant que le rejeu est terminé même lorsqu’il est vide. Borner les files et traiter correctement la course entre historique et événements live.
* Reprendre les Change Streams depuis le checkpoint quand possible. Si la reprise est impossible, rattraper les événements manqués depuis le journal durable avec une stratégie d’ordre documentée.
* Gérer annulation de contexte, backpressure, erreurs transitoires, reconnexion, fermeture des cursors, goroutines et client MongoDB. Les abonnés/socket SSE restent en mémoire ; ne pas les persister dans MongoDB.

## Étape 5 — Tests et validation technique

### Tests unitaires

* Sérialisation/désérialisation et versionnement du document d’événement.
* Ordre, pagination, indexation, rétention et recherche du curseur.
* Rejeu avec ID présent, absent, répété, inconnu ou expiré.
* Courses entre rejeu et live, publications simultanées, redémarrages et doublons potentiels.
* Filtrage par topic, plusieurs topics, comportement d’autorisation conforme au hub.
* Erreurs MongoDB, token de reprise invalide, files pleines, backpressure et fermeture propre.

### Tests d’intégration

* Exécuter contre un replica set de test, pas seulement un MongoDB standalone.
* Vérifier la publication et le rejeu après redémarrage d’un hub.
* Tester deux processus Mercure partageant MongoDB : publication sur A, réception par un abonné connecté à B.
* Simuler une interruption plus longue que la fenêtre d’oplog et valider le rattrapage depuis le journal durable.
* Vérifier les index, la rétention, les permissions minimales et la fermeture des ressources.

### Test de charge SSE

* Tester plusieurs milliers de connexions ouvertes et faire varier indépendamment le nombre de clients, le débit de publication et le fan-out par topic.
* Mesurer connexions actives, CPU/mémoire, descripteurs de fichiers, latence publication→client, retard Change Stream, charge MongoDB, rejeu et reconnexions.
* Comparer une instance et plusieurs instances ; publier la configuration matérielle, les paramètres et les limites. Ne pas présenter un benchmark comme une garantie universelle.

## Étape 6 — README principal : documentation et présentation du projet

Créer un README de qualité professionnelle qui soit à la fois la page d’accueil du dépôt et le guide d’utilisation principal. Il doit être agréable à parcourir, concis dans sa présentation et suffisamment précis pour qu’un utilisateur puisse compiler, configurer, lancer et dépanner le transport sans lire le code. Ne pas créer de site GitHub Pages ni disperser la documentation utilisateur dans un site séparé.

Inclure dans le README, dans un ordre facile à suivre :

* Un titre, une description courte de la valeur apportée, le statut du projet, badges pertinents (build, licence, dernière release seulement s’ils sont réels) et une table des matières.
* Un schéma d’architecture Mermaid soigné, rendu directement sur GitHub, avec les publishers, les instances Mercure, la collection d’événements MongoDB, les Change Streams, le rejeu `Last-Event-ID` et les clients SSE. Utiliser des sous-graphes/étiquettes lisibles et distinguer clairement stockage durable, notifications inter-nœuds et abonnés gardés en mémoire. Vérifier le rendu Mermaid et l’exactitude du flux représenté ; ne pas ajouter de dépendance de site ou de générateur.
* Fonctionnalités, non-objectifs et limites connues, notamment exigences MongoDB, Change Streams, ordre, rétention et sémantique de reprise.
* Quickstart copiable : prérequis, démarrage d’un replica set de développement, variables d’environnement, build du binaire Mercure/Caddy avec le module, configuration, démarrage du hub, exemple de publication HTTP et exemple d’abonnement SSE.
* Configuration complète : options réellement prises en charge, exemples Caddyfile/JSON compatibles avec la version cible, variables d’environnement et bonnes pratiques pour les secrets.
* Conseils de production : permissions Mongo minimales, index, taille/fenêtre d’oplog, rétention, reprise, multi-instance, proxy SSE, timeouts, heartbeat, observabilité et dépannage.
* Garantie de livraison et comportement `Last-Event-ID`, sauvegarde/récupération, compatibilité Mercure/Caddy/Go, tests et instructions de contribution.
* Licence et avertissement sur les obligations AGPL après vérification.

Soigner la hiérarchie visuelle avec des titres courts, des blocs de code testés, des tableaux seulement quand utiles et des liens internes cohérents. S’inspirer du niveau de finition des pages produit modernes comme Mercure.rocks sans copier leur marque ou leurs éléments. Toute commande, variable et directive montrée doit exister et être testée autant que possible en CI.

## Étape 7 — Release et critères d’acceptation

* Dépôt GitHub `GromNaN/mercure-mongodb` créé ou checkout existant utilisé sans perte de données.
* Module séparé compilable avec la version Mercure/Caddy documentée ; aucun fork du protocole/hub requis pour l’intégration visée.
* Un événement publié est stocké dans MongoDB et rejouable selon un contrat décrit et testé.
* Plusieurs hubs synchronisent les publications via MongoDB sans Redis, Kafka ou autre service de stockage externe.
* Reconnexion, rétention, perte d’un token Change Stream, frontière replay/live et comportement multi-instance sont testés et documentés.
* Tests d’intégration sur replica set ; test de charge couvrant plusieurs milliers de connexions SSE et le fan-out.
* README complet, agréable à lire, avec schéma Mermaid vérifié et exemples d’utilisation/configuration testés.
* Aucun site GitHub Pages ni workflow de déploiement de site ajouté.
* Licence, versions supportées, limites et commande de build clairement annoncées.

## Instructions d’exécution à Claude Code

Commence par l’étape 0 : crée ou récupère `GromNaN/mercure-mongodb` sans écraser l’existant. Si l’authentification ou les permissions empêchent la création, arrête-toi et demande à l’utilisateur de créer le dépôt ou de fournir l’accès. Ensuite, avance dans l’ordre, inspecte le code avant d’implémenter, consigne les décisions dans l’ADR, et valide chaque livrable par des tests/builds. Signale explicitement tout blocage ou tout point non vérifié.
