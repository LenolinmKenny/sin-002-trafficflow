package co.wethinkcode.trafficflow;

import io.javalin.Javalin;

public class IngestionServiceApp {

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7020);

        app.get("/health", ctx -> ctx.result("OK"));
        // TODO: read and clean src/main/resources/intersections-legacy.csv (intersections, districts, signal types data —
        // trim whitespace, fix casing, normalize dates/booleans) and expose the
        // cleaned records here for the other services to consume.
    }

    private static final List<String> PLACEHOLDERS = List.of("n/a", "unknown", "null", "nil", "none", "");

    /**
      Parses and cleans the CSV from the supplied reader.

      Expected header: intersection_id, District, signal_type, active_flag
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
                cleaned.merge(id, candidate, CsvProcessor::merge);
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

    /**
     * Merge two records that share a normalized ID.
     * - district / signalType: keep the first non-null value.
     * - active: OR (if either duplicate claimed active, treat as active).
     */
    private static Intersection merge(Intersection a, Intersection b) {
        return new Intersection(
                a.id(),
                a.district()   != null ? a.district()   : b.district(),
                a.signalType() != null ? a.signalType() : b.signalType(),
                a.active() || b.active()
        );
    }

    private static String toJson(List<Intersection> records) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < records.size(); i++) {
            Intersection r = records.get(i);
            if (i > 0) sb.append(",");
            sb.append("{")
                    .append("\"id\":").append(quote(r.id())).append(",")
                    .append("\"district\":").append(quote(r.district())).append(",")
                    .append("\"signalType\":").append(quote(r.signalType())).append(",")
                    .append("\"active\":").append(r.active())
                    .append("}");
        }
        return sb.append("]").toString();
    }

    private static String quote(String s) {
        if (s == null) return "null";
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}

