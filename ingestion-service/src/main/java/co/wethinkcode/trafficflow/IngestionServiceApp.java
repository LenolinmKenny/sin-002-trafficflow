package co.wethinkcode.trafficflow;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.javalin.Javalin;

public class IngestionServiceApp {

    public record Intersection(String id, String district, String signalType, boolean active) {}

    private static final List<String> PLACEHOLDERS = List.of("n/a", "unknown", "null", "nil", "none", "");

    public static void main(String[] args) throws Exception {
        List<Intersection> intersections;
        try (var in = IngestionServiceApp.class
                .getResourceAsStream("/intersections-legacy.csv")) {
            if (in == null) throw new IllegalStateException("CSV not on classpath");
            intersections = cleanCsv(new InputStreamReader(in, StandardCharsets.UTF_8));
        }

        Javalin app = Javalin.create().start(7020);
        app.get("/health",        ctx -> ctx.result("OK"));
     //   app.get("/intersections", ctx -> ctx.json(intersections));
    }

    /**
     * Parses and cleans the CSV from the supplied reader.
     *
     * Expected header: intersection_id, District, signal_type, active_flag
     */
    public static List<Intersection> cleanCsv(Reader reader) throws IOException, CsvValidationException {
        Map<String, Intersection> cleaned = new LinkedHashMap<>();

        try (CSVReader csv = new CSVReader(reader)) {
            csv.readNext();
            String[] row;
            while ((row = csv.readNext()) != null) {
                if (row.length < 4) continue;

                String id = normalizeId(row[0]);
                if (id == null) continue;

                Intersection candidate = new Intersection(
                        id,
                        cleanString(row[1]),
                        cleanSignalType(row[2]),
                        parseActive(row[3])
                );
                cleaned.merge(id, candidate, IngestionServiceApp::merge);
            }
        }
        return new ArrayList<>(cleaned.values());
    }

    /** Trim, collapse whitespace, uppercase. Returns null if blank/placeholder. */
    private static String normalizeId(String raw) {
        String s = cleanString(raw);
        return s == null ? null : s.toUpperCase();
    }

    /** Trim edges, collapse internal runs of whitespace, map placeholders → null. */
    private static String cleanString(String raw) {
        if (raw == null) return null;
        String s = raw.trim().replaceAll("\\s+", " ");
        return PLACEHOLDERS.contains(s.toLowerCase()) ? null : s;
    }

    /** Signal type: cleaned, lowercased. No alias map — value is fixed in the source. */
    private static String cleanSignalType(String raw) {
        String s = cleanString(raw);
        return s == null ? null : s.toLowerCase();
    }

    /** Truthy = y/yes/1/true (case-insensitive). Everything else → false. */
    private static boolean parseActive(String raw) {
        if (raw == null) return false;
        return raw.trim().toLowerCase().matches("^(y|yes|1|true)$");
    }

    /** Merge two records that share a normalized ID. */
    private static Intersection merge(Intersection a, Intersection b) {
        return new Intersection(
                a.id(),
                a.district()   != null ? a.district()   : b.district(),
                a.signalType() != null ? a.signalType() : b.signalType(),
                a.active() || b.active()
        );
    }
}

