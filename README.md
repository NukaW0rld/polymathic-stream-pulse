# POLYMATHIC Stream Pulse

A Twitch analytics project that captures live stream data and analyzes how audience and community activity evolve throughout long-form DJ streams.

The project is being built for POLYMATHIC, a Drum & Bass DJ and Twitch Ambassador, with the goal of producing insights beyond Twitch's standard Creator Dashboard while serving as a practical data-engineering and analytics portfolio project.

## Goals

The system will collect live Twitch data during streams and use it to analyze areas such as:

* audience and chat activity over the course of a stream;
* differences between stream days and stream durations;
* incoming raid impact;
* returning and recurring chat participants;
* follower and engagement patterns where the available data supports them.

## Planned stack

* Python
* PostgreSQL
* SQL
* Pandas
* Power BI

The long-running collector is expected to run on Linux, with Power BI development performed on Windows.

## Project status

Early development.

The initial priority is building a reliable data-collection pipeline and collecting trustworthy live data before developing the final analytical model and dashboard.

## Data and privacy

Production data may contain Twitch usernames, user IDs, and raw chat messages and will remain private.

This public repository will contain only anonymized, aggregated, sanitized, or synthetic data.

Credentials, OAuth tokens, raw production data, and identifiable user data will never be committed.

## Scope

The first version is intentionally focused on descriptive and diagnostic analytics rather than machine learning, prediction, or a public web application.

More detailed architecture, methodology, findings, and dashboard documentation will be added as the project develops.
