package it.mobimesh.usermigration;

import it.mobimesh.wp.extdata.UserExtendedData;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.text.WordUtils;

public class UserMigration {

    final static String KEY_LAST_VISIT = "last_visit_ts";
    final static String KEY_SIGNUP = "signup_ts";
    final static String KEY_PROVIDER = "provider";

    final static HashSet<String> ALREADY_IN_USERNAMES = new HashSet<>(100);

    static boolean stopOnCsvError = true;

    public static void main(String[] args) {

        String csv = "/tmp/account.csv"; // <---- FILE DA CUI LEGGERE TUTTI I DATI

        // Altre variabili da configurare sono più in basso e sono:
        //  - sqlTenantId
        //  - sqlOkDomain
        //  - sqlDupDomain
        //  - sqlDefaultPassword
        // Questi username verranno skippati (perché ga' nel DB, magari per test, ecc.)
        // se vuoi cercarli usa la query 'SELECT username FROM aaa_account WHERE domain = "REGISTERED";
        ALREADY_IN_USERNAMES.add("+39123456789108");
        /*
        ALREADY_IN_USERNAMES.add("+393271312425");
        ALREADY_IN_USERNAMES.add("+393293323946");
        ALREADY_IN_USERNAMES.add("+393332131528");
        ALREADY_IN_USERNAMES.add("+393333424585");
        ALREADY_IN_USERNAMES.add("+393337501972");
        ALREADY_IN_USERNAMES.add("+393357321496");
        ALREADY_IN_USERNAMES.add("+393357533308");
        ALREADY_IN_USERNAMES.add("+393358336819");
        ALREADY_IN_USERNAMES.add("+393371115572");
        ALREADY_IN_USERNAMES.add("+393388540597");
        ALREADY_IN_USERNAMES.add("+393486052422");
        ALREADY_IN_USERNAMES.add("+393492115198");
        ALREADY_IN_USERNAMES.add("+393666216849");
         */

        for (String arg : args) {

            if (arg.startsWith("csv=")) {
                csv = arg.replaceFirst("csv=", "");
            } else {
                System.err.println("Unknwon arg: '" + arg + "'.");
            }
        }

        readCsv(csv);
    }

    private static void readCsv(String path) {

        DateTimeFormatter signupDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter lastVisitDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        Iterable<CSVRecord> records = null;

        try {

            Reader in = new FileReader(path);

            // https://commons.apache.org/proper/commons-csv/user-guide.html
            CSVFormat format = CSVFormat.newFormat(';')
                    .withQuote('"')
                    .withRecordSeparator("\n")
                    .withIgnoreEmptyLines(true)
                    .withAllowMissingColumnNames(true).withHeader("Marketing clause accepted",
                    "Username",
                    "First Name",
                    "Last Name",
                    "Gender",
                    "Mobile Phone",
                    "Email",
                    "Location",
                    "Birthday",
                    "Country",
                    "City",
                    "Address",
                    "Identification",
                    "ID Number",
                    "Sign-Up Option",
                    "Social Network username",
                    "Venues",
                    "Followers",
                    "Friends",
                    "Age",
                    "User Status",
                    "Last Visit",
                    "Signup",
                    "Device",
                    "Visits",
                    "Company",
                    "Validation",
                    "Internet Plan",
                    "Job Title",
                    "Type",
                    "Personal ID",
                    "Passport Number",
                    "Member ID");

            records = format.parse(in);
        } catch (Exception ex) {

            System.err.println("An error occurred while opening CSV.");
            ex.printStackTrace();
        }

        if (records == null) {

            System.err.println("Unable to get CSV records.");
            return;
        }

        long totalRows = 0;
        long notUserAccount = 0;
        long skipped = 0;
        long duplicated = 0;
        long marketingFlag = 0;

        HashMap<String, UserExtendedData> convertedData = new HashMap<>(500000);
        HashMap<String, UserExtendedData> garbageData = new HashMap<>(100000);
        HashMap<String, TreeMap<Long, UserExtendedData>> duplicatedData = new HashMap<>(100000);

        try {

            for (CSVRecord record : records) {

                totalRows++;
                boolean isADuplicate = false;

                final String signupOption = record.get("Sign-Up Option");

                if (!"User Account".equals(signupOption)) {

                    notUserAccount++;
                    continue;
                }

                String name = record.get("First Name");
                String surname = record.get("Last Name");

                if (name != null) {
                    name = WordUtils.capitalize(name.trim().toLowerCase());
                }

                if (surname != null) {
                    surname = WordUtils.capitalize(surname.trim().toLowerCase());
                }

                String mobilePhone = record.get("Mobile Phone");

                if (mobilePhone == null || mobilePhone.isEmpty()) {

                    skipped++;
                    System.err.println("SKIPPING - No mobile phone (name: " + name + ", surname: " + surname + ")");
                    continue;
                }

                mobilePhone = mobilePhone.replaceAll("\\s+", "");
                mobilePhone = mobilePhone.replaceAll("\\.", "");
                mobilePhone = mobilePhone.replaceAll("\\(", "");
                mobilePhone = mobilePhone.replaceAll("\\)", "");
                mobilePhone = mobilePhone.replaceAll("E\\+\\d", ""); //Alcuni numeri sono tipo Excel "393564211467733E+17"

                if (!mobilePhone.matches("\\+?[0-9]+")) {

                    skipped++;
                    System.err.println("SKIPPING - Invalid mobile phone \" " + mobilePhone + " \" (name: " + name + ", surname: " + surname + ")");
                    continue;
                }

                String fixedMobilePhone = mobilePhone;

                if (fixedMobilePhone.startsWith("00")) {
                    fixedMobilePhone = fixedMobilePhone.replaceFirst("00", "+");
                } else if (!fixedMobilePhone.startsWith("+")) {
                    fixedMobilePhone = "+" + fixedMobilePhone;
                }

                String email = record.get("Email");

                if (email == null || email.isEmpty()) {

                    skipped++;
                    System.err.println("SKIPPING - No email (name: " + name + ", surname: " + surname + ")");
                    continue;
                } else {
                    email = email.trim();
                }

                final LocalDateTime signupDate = LocalDateTime.parse(record.get("Signup"), signupDateTimeFormatter);
                long signupTs = Timestamp.valueOf(signupDate).getTime() / 1000;

                if (!fixedMobilePhone.startsWith("+")) {

                    skipped++;
                    System.err.println("SKIPPING - Strange mobile phone '" + mobilePhone + "' (name: " + name + ", surname: " + surname + ")");
                    continue;
                }
                /*
                else if (!fixedMobilePhone.startsWith("+39")) {

                    skipped++;
                    System.err.println("SKIPPING - Not Italian mobile phone '" + mobilePhone + "' (name: " + name + ", surname: " + surname + ")");
                    continue;
                }
                 */

                if (ALREADY_IN_USERNAMES.contains(fixedMobilePhone)) {

                    skipped++;
                    System.err.println("SKIPPING - User already in database '" + fixedMobilePhone + "' (name: " + name + ", surname: " + surname + ") maybe for test?");
                    continue;
                }

                final String lastVisitString = record.get("Last Visit");
                long lastVisitTs = 0L;

                if (!"0000-00-00".equals(lastVisitString)) {

                    final LocalDate lastVisitDate = LocalDate.parse(lastVisitString, lastVisitDateTimeFormatter);
                    lastVisitTs = lastVisitDate.atStartOfDay().toEpochSecond(ZoneOffset.UTC);
                }

                if (duplicatedData.keySet().contains(fixedMobilePhone)) {
                    isADuplicate = true;
                } else if (convertedData.keySet().contains(fixedMobilePhone)) {

                    duplicated++;
                    isADuplicate = true;
                }

                String marketing = record.get("Marketing clause accepted");
                boolean marketingAccepted = ("yes".equalsIgnoreCase(marketing));
                if (marketingAccepted) {
                    marketingFlag++;
                }

                UserExtendedData extData = new UserExtendedData();

                HashMap<String, String> verifiedData = new HashMap<>(2);
                verifiedData.put(UserExtendedData.KEY_PHONE_NUMBER, fixedMobilePhone);
                verifiedData.put(KEY_PROVIDER, "witech");
                extData.setVerifiedData(verifiedData);

                HashMap<String, String> formData = new HashMap<>(4);
                formData.put(UserExtendedData.KEY_SURNAME, surname);
                formData.put(UserExtendedData.KEY_NAME, name);
                formData.put(UserExtendedData.KEY_E_MAIL, email);
                formData.put(UserExtendedData.KEY_PHONE_NUMBER, fixedMobilePhone);
                if (marketingAccepted) {
                    formData.put(UserExtendedData.KEY_MARKETING, "true");
                }
                formData.put(UserExtendedData.KEY_TERMS, "true");
                extData.setFormData(formData);

                HashMap<String, String> operatorData = new HashMap<>(3);
                operatorData.put(KEY_SIGNUP, "" + signupTs);
                operatorData.put(KEY_LAST_VISIT, "" + lastVisitTs);
                extData.setOperatorData(operatorData);

                if (!isADuplicate) {
                    convertedData.put(fixedMobilePhone, extData);
                } else if (duplicatedData.keySet().contains(fixedMobilePhone)) {

                    boolean inserted = false;
                    TreeMap<Long, UserExtendedData> others = duplicatedData.get(fixedMobilePhone);

                    for (int i = 0; i < 999; i++) {

                        long newTs = lastVisitTs + i;

                        if (!others.containsKey(newTs)) {

                            others.put(newTs, extData);
                            inserted = true;
                            break;
                        }

                    }

                    if (!inserted) {

                        System.err.println("ERROR - Cannot create a valid key for duplicated user: '" + fixedMobilePhone + "' (name: " + name + ", surname: " + surname + ")");
                        return;
                    }

                } else {

                    UserExtendedData originalExtData = convertedData.remove(fixedMobilePhone);
                    long originalLastVisitTs = Long.parseLong(originalExtData.getOperatorData(KEY_LAST_VISIT));

                    // Creo una mappa ordinata (e ordino al contrario così ho l'ultimo ts come primo elemento)
                    TreeMap<Long, UserExtendedData> others = new TreeMap<>(Collections.reverseOrder());

                    if (lastVisitTs == originalLastVisitTs) {
                        lastVisitTs++;
                    }

                    others.put(originalLastVisitTs, originalExtData);
                    others.put(lastVisitTs, extData);

                    duplicatedData.put(fixedMobilePhone, others);
                }
            }
        } catch (IllegalStateException ex) {

            System.err.println("WARNING - Invalid CSV line for record " + totalRows);

            if (stopOnCsvError) {
                throw ex;
            }
        }

        System.out.println("");
        System.out.println("TOTAL ROWS:   " + totalRows + "\t(" + String.format("%.1f%%", 100.0 * totalRows / totalRows) + ")");

        System.out.println("");
        System.out.println("NOT USER ACC: " + notUserAccount + (notUserAccount < 10 ? "\t" : "") + "\t(" + String.format("%.1f%%", 100.0 * notUserAccount / totalRows) + ")");
        System.out.println("INVALID/SKIP: " + skipped + "\t(" + String.format("%.1f%%", 100.0 * skipped / totalRows) + ")");
        //System.out.println("DUPLICATED:   " + duplicated + "\t(" + String.format("%.1f%%", 100.0 * duplicated / totalRows) + ")");
        //System.out.println("DUP MAP:      " + duplicatedData.size() + "\t(" + String.format("%.1f%%", 100.0 * duplicatedData.size() / totalRows) + ")");

        long totalDuplicates = 0;

        for (Map.Entry<String, TreeMap<Long, UserExtendedData>> tempData : duplicatedData.entrySet()) {
            totalDuplicates += tempData.getValue().size();
        }

        long onceAccounts = convertedData.size();
        long rightDuplicates = duplicatedData.size();
        long rightAccounts = convertedData.size() + duplicatedData.size();

        System.out.println("TOTAL DUPS:   " + totalDuplicates + "\t(" + String.format("%.1f%%", 100.0 * totalDuplicates / totalRows) + ")");
        //System.out.println("RIGHT DUPS:   " + rightDuplicates + "\t(" + String.format("%.1f%%", 100.0 * rightDuplicates / totalRows) + ")");
        System.out.println("TOTAL ONCE:   " + onceAccounts + "\t(" + String.format("%.1f%%", 100.0 * onceAccounts / totalRows) + ")");
        System.out.println("[ MARKETING:  " + marketingFlag + "\t(" + String.format("%.1f%%", 100.0 * marketingFlag / totalRows) + ") ]");

        System.out.println("");
        System.out.println("TOTAL ACC:    " + rightAccounts + "\t(" + String.format("%.1f%%", 100.0 * rightAccounts / totalRows) + ") with duplicated taken once");

        // Controllo i duplicati
        for (Map.Entry<String, TreeMap<Long, UserExtendedData>> tempData : duplicatedData.entrySet()) {

            TreeMap<Long, UserExtendedData> others = tempData.getValue();

            HashMap<String, Integer> names = new HashMap<>();
            HashMap<String, Integer> surnames = new HashMap<>();

            // Qui sto iterando sui dati di un doppione dall'ultima visita alla prima
            for (Map.Entry<Long, UserExtendedData> other : others.entrySet()) {

                //Long lastVisitTs = other.getKey();
                UserExtendedData extData = other.getValue();

                final String name = extData.getFormData(UserExtendedData.KEY_NAME);
                final String surname = extData.getFormData(UserExtendedData.KEY_SURNAME);

                if (name != null) {

                    if (names.containsKey(name)) {

                        Integer tot = names.get(name);
                        names.put(name, tot + 1);
                    } else {
                        names.put(name, 1);
                    }
                }

                if (surname != null) {

                    if (surnames.containsKey(surname)) {

                        Integer tot = surnames.get(surname);
                        surnames.put(surname, tot + 1);
                    } else {
                        surnames.put(surname, 1);
                    }
                }
            }

            String bestName = null;
            int bestNameScore = 0;
            String bestSurname = null;
            int bestSurnameScore = 0;

            String allNames = "'";
            String allSurnames = "'";

            for (Map.Entry<String, Integer> name : names.entrySet()) {

                if (name.getValue() > bestNameScore) {

                    bestName = name.getKey();
                    bestNameScore = name.getValue();
                }

                allNames += " " + name.getKey() + "," + name.getValue();
            }

            for (Map.Entry<String, Integer> surname : surnames.entrySet()) {

                if (surname.getValue() > bestSurnameScore) {

                    bestSurname = surname.getKey();
                    bestSurnameScore = surname.getValue();
                }

                allSurnames += " " + surname.getKey() + "," + surname.getValue();
            }

            allNames += "'";
            allSurnames += "'";

            /*
            // Decommentare qui per vedere i nomi duplicati
            System.out.println("WARNING - " + others.size() + " registrations for user: '" + tempData.getKey() + "' (name: " + allNames + ", surname: " + allSurnames + ")");
            System.out.println("        - choosing '" + bestName + " " + bestSurname + "'");
             */
            String userMobilePhone = tempData.getKey();

            /*
            // Decommentare qui per mettere tra gli utenti buoni quello
            // con il nome più promettente
            UserExtendedData latestExtData = others.entrySet().iterator().next().getValue();

            latestExtData.setFormData(UserExtendedData.KEY_NAME, bestName);
            latestExtData.setFormData(UserExtendedData.KEY_SURNAME, bestSurname);

            convertedData.put(userMobilePhone, latestExtData);
             */
            // Tengo un progressive per numerare le chiavi dei duplicati
            int progressive = -1;

            for (Map.Entry<Long, UserExtendedData> other : others.entrySet()) {

                progressive++;

                // Se progressive == 0 utente da tenere buono
                if (progressive == 0) {
                    convertedData.put(userMobilePhone, other.getValue());
                } // Se progressive != 0 utente garbage
                else {
                    garbageData.put(userMobilePhone + "_" + progressive, other.getValue());
                }
            }
        }

        System.out.println("");
        System.out.println("CONVERTED:    " + convertedData.size());
        System.out.println("GARBAGE:      " + garbageData.size());

        System.out.println("");
        System.out.println("TOTAL (C+G):  " + (convertedData.size() + garbageData.size()));
        System.out.println("TOT+INV+SKIP: " + (convertedData.size() + garbageData.size() + notUserAccount + skipped));

        // GENERAZIONE FILE SQL
        final String sqlOutputFilePath = "/tmp/import.sql";
        PrintWriter pw = null;

        try {

            pw = new PrintWriter(new FileWriter(sqlOutputFilePath));
        } catch (Exception ex) {

            System.err.println("ERROR - Cannot write to output file...");
            return;
        }

        final int sqlTenantId = 1;
        final String sqlOkDomain = "REGISTERED";
        final String sqlDupDomain = "DUPLICATED";
        final String sqlDefaultPassword = "SHA256:0000000000000000000000000000000000000000000000000000000000000000";

        pw.write("\n -- REAL USER DATA -- \n\n");

        Iterator<Map.Entry<String, UserExtendedData>> itc = convertedData.entrySet().iterator();
        Iterator<Map.Entry<String, UserExtendedData>> itg = garbageData.entrySet().iterator();

        System.out.println("");
        System.out.println("INFO - Started writing '" + sqlOutputFilePath + "'");

        int j = 1;

        pw.write("INSERT INTO aaa_account(tenant, domain, username, password, creation_ts, last_password_ts, last_authentication_ts, last_authentication_with_password_ts, extended_data) VALUES \n");

        while (itc.hasNext()) {

            final Map.Entry<String, UserExtendedData> entry = itc.next();
            final UserExtendedData extData = entry.getValue();

            final String sqlUsername = entry.getKey();

            long sqlCreationTs = 0;                         // Se non c'è, in SQL mettere 0
            long sqlLastPasswordTs = 0;                     // Visto che tutti dovranno recuperare la password mettere 0 (e poi nell'AAA mettere password expiration != NULL)
            long sqlLastAuthenticationTs = 0;               // Se non c'è, in SQL mettere 0
            long sqlLastAuthenticationWithPasswordTs = 0;   // Da mettere identico a sqlLastAuthenticationTs, tranne se non c'è, solo qui, mettere NULL

            try {
                sqlCreationTs = Long.parseLong(extData.getOperatorData(KEY_SIGNUP));
            } catch (Exception ex) {
            }
            if (sqlCreationTs < 9999) {
                sqlCreationTs = 0;
            }

            try {
                sqlLastAuthenticationTs = Long.parseLong(extData.getOperatorData(KEY_LAST_VISIT));
            } catch (Exception ex) {
            }
            if (sqlLastAuthenticationTs < 9999) {
                sqlLastAuthenticationTs = 0;
            }
            sqlLastAuthenticationWithPasswordTs = sqlLastAuthenticationTs;

            // Rimuovo gli operator data (li avevo messi solo per comodità)
            extData.clearOperatorData();

            final String sqlExtData = extData.toEscapedSingleQuotedString();

            pw.format("(%d, '%s', '%s',\t'%s', %d, %d, %d, %s, '%s')",
                    sqlTenantId,
                    sqlOkDomain, // <--- occhio al dominio
                    sqlUsername,
                    sqlDefaultPassword,
                    sqlCreationTs,
                    sqlLastPasswordTs,
                    sqlLastAuthenticationTs,
                    (sqlLastAuthenticationWithPasswordTs != 0 ? "" + sqlLastAuthenticationWithPasswordTs : "NULL"),
                    sqlExtData);

            // Qui spezzo in blocchi da 1000
            if (!itc.hasNext()) {
                pw.write(";\n");
            } else if (j % 1000 == 0) {
                pw.write(";\n\nINSERT INTO aaa_account(tenant, domain, username, password, creation_ts, last_password_ts, last_authentication_ts, last_authentication_with_password_ts, extended_data) VALUES \n");
            } else {
                pw.write(",\n");
            }

            j++;
        }

        System.out.println("INFO - Ended writing real user data");

        pw.write("\n -- GARBAGE DATA -- \n\n");

        System.out.println("INFO - Started writing garbage data");

        j = 1;

        pw.write("INSERT INTO aaa_account(tenant, domain, username, password, creation_ts, last_password_ts, last_authentication_ts, last_authentication_with_password_ts, extended_data) VALUES \n");

        while (itg.hasNext()) {

            final Map.Entry<String, UserExtendedData> entry = itg.next();
            final UserExtendedData extData = entry.getValue();

            final String sqlUsername = entry.getKey();

            long sqlCreationTs = 0;                         // Se non c'è, in SQL mettere 0
            long sqlLastPasswordTs = 0;                     // Visto che tutti dovranno recuperare la password mettere 0 (e poi nell'AAA mettere password expiration != NULL)
            long sqlLastAuthenticationTs = 0;               // Se non c'è, in SQL mettere 0
            long sqlLastAuthenticationWithPasswordTs = 0;   // Da mettere identico a sqlLastAuthenticationTs, tranne se non c'è, solo qui, mettere NULL

            try {
                sqlCreationTs = Long.parseLong(extData.getOperatorData(KEY_SIGNUP));
            } catch (Exception ex) {
            }
            if (sqlCreationTs < 9999) {
                sqlCreationTs = 0;
            }

            try {
                sqlLastAuthenticationTs = Long.parseLong(extData.getOperatorData(KEY_LAST_VISIT));
            } catch (Exception ex) {
            }
            if (sqlLastAuthenticationTs < 9999) {
                sqlLastAuthenticationTs = 0;
            }
            sqlLastAuthenticationWithPasswordTs = sqlLastAuthenticationTs;

            // Rimuovo gli operator data (li avevo messi solo per comodità)
            extData.clearOperatorData();

            final String sqlExtData = extData.toEscapedSingleQuotedString();

            pw.format("(%d, '%s', '%s',\t'%s', %d, %d, %d, %s, '%s')",
                    sqlTenantId,
                    sqlDupDomain, // <--- occhio al dominio
                    sqlUsername,
                    sqlDefaultPassword,
                    sqlCreationTs,
                    sqlLastPasswordTs,
                    sqlLastAuthenticationTs,
                    (sqlLastAuthenticationWithPasswordTs != 0 ? "" + sqlLastAuthenticationWithPasswordTs : "NULL"),
                    sqlExtData);

            // Qui spezzo in blocchi da 1000
            if (!itg.hasNext()) {
                pw.write(";\n");
            } else if (j % 1000 == 0) {
                pw.write(";\n\nINSERT INTO aaa_account(tenant, domain, username, password, creation_ts, last_password_ts, last_authentication_ts, last_authentication_with_password_ts, extended_data) VALUES \n");
            } else {
                pw.write(",\n");
            }

            j++;
        }

        System.out.println("INFO - Ended writing garbage data");

        pw.close();
    }
}
