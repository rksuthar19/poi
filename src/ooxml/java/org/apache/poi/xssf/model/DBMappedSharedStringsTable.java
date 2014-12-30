/* ====================================================================
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
==================================================================== */

package org.apache.poi.xssf.model;

import org.apache.poi.util.TempFile;
import org.apache.xmlbeans.XmlOptions;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTRst;

import javax.xml.stream.XMLStreamException;
import java.io.*;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Collections;

/**
 * SharedStringsTable With Map DB implementation
 * To reduce memory footprint of POI’s shared strings table
 * It flows data to disk as per availability of memory (Reference)
 */
public class DBMappedSharedStringsTable extends SharedStringsTable implements AutoCloseable {

    /**
     * Maps strings and their indexes in the <code>stringVsIndexSTMap</code> map db
     */
    private DB stringVsIndexMapDB;
    private HTreeMap<String, Integer> stringVsIndexSTMap; //string vs index map to lookup existing record in stTable look at add entry method
    /**
     * Maps strings and their indexes in the <code>stringVsIndexSTMap</code> map db
     */
    private DB indexVsStringMapDB;
    private HTreeMap<Integer, String> indexVsStringSTMap; //index vs string map to retrieve record with index

    private final File temp_shared_string_file;

    /**
     * An integer representing the total count of strings in the workbook. This count does not
     * include any numbers, it counts only the total of text strings in the workbook.
     */
    private int count;

    /**
     * An integer representing the total count of unique strings in the Shared String Table.
     * A string is unique even if it is a copy of another string, but has different formatting applied
     * at the character level.
     */
    private int uniqueCount;

    private final static XmlOptions options = new XmlOptions();
    private final static XmlOptions out_options = new XmlOptions();


    static {
        options.put(XmlOptions.SAVE_INNER);
        options.put(XmlOptions.SAVE_AGGRESSIVE_NAMESPACES);
        options.put(XmlOptions.SAVE_USE_DEFAULT_NAMESPACE);
        options.setSaveImplicitNamespaces(Collections.singletonMap("", "http://schemas.openxmlformats.org/spreadsheetml/2006/main"));
    }

    public DBMappedSharedStringsTable() {
        super();
        temp_shared_string_file = createTempFile("poi-shared-string-table", ".xml");
        initMapDbBasedSharedStringTableMap();
    }

    public FileInputStream getSharedStringInputStream() throws IOException {
        return new FileInputStream(temp_shared_string_file);
    }

    public FileOutputStream getSharedStringsTableOutputStream() throws IOException {
        return new FileOutputStream(temp_shared_string_file);
    }

    public File getTemp_shared_string_file() {
        return temp_shared_string_file;
    }

    private File createTempFile(String prefix, String suffix) {
        try {
            return TempFile.createTempFile(prefix, suffix);
        } catch (IOException e) {
            throw new RuntimeException("Couldn't create required temp file", e);
        }
    }

    private void initMapDbBasedSharedStringTableMap() {
        initStringVsIndexBasedMapDB();
        initIndexVsStringBasedMapDB();
    }

    private void initStringVsIndexBasedMapDB() {
        int HARD_REF_CACHE_INITIAL_CAPACITY = 65536;//for HardRef cache it is initial capacity of underlying table (HashMap) Default cache size is 32768 setting it to 65536
        stringVsIndexMapDB = DBMaker.newFileDB(createTempFile("stringVsIndexMapDBFile", ""))
                .transactionDisable()
                .cacheHardRefEnable()
                .cacheSize(HARD_REF_CACHE_INITIAL_CAPACITY)
                .deleteFilesAfterClose()
                .mmapFileEnablePartial()
                .closeOnJvmShutdown().make();
        stringVsIndexSTMap = stringVsIndexMapDB.createHashMap(new BigInteger(130, new SecureRandom()).toString(32)).make();
    }

    private void initIndexVsStringBasedMapDB() {
        indexVsStringMapDB = DBMaker.newFileDB(createTempFile("indexVsStringMapDBFile", ""))
                .transactionDisable()
                .cacheDisable() //caching not required indexVsStringSTMap will be used to write all existing values
                .deleteFilesAfterClose()
                .mmapFileEnablePartial()
                .closeOnJvmShutdown().make();
        indexVsStringSTMap = indexVsStringMapDB.createHashMap(new BigInteger(130, new SecureRandom()).toString(32)).make();
    }

    private String getKey(CTRst st) {
        return st.xmlText(options);
    }

    /**
     * Return an integer representing the total count of strings in the workbook. This count does not
     * include any numbers, it counts only the total of text strings in the workbook.
     *
     * @return the total count of strings in the workbook
     */
    @Override
    public int getCount() {
        return count;
    }

    /**
     * Returns an integer representing the total count of unique strings in the Shared String Table.
     * A string is unique even if it is a copy of another string, but has different formatting applied
     * at the character level.
     *
     * @return the total count of unique strings in the workbook
     */
    @Override
    public int getUniqueCount() {
        return uniqueCount;
    }

    /**
     * Add an entry to this Shared String table (a new value is appened to the end).
     * <p/>
     * <p>
     * If the Shared String table already contains this <code>CTRst</code> bean, its index is returned.
     * Otherwise a new entry is aded.
     * </p>
     *
     * @param st the entry to add
     * @return index the index of added entry
     */
    @Override
    public int addEntry(CTRst st) {
        String s = getKey(st);
        count++;
        if (stringVsIndexSTMap.containsKey(s)) {
            return stringVsIndexSTMap.get(s);
        }
        //new unique record
        stringVsIndexSTMap.put(s, uniqueCount);
        indexVsStringSTMap.put(uniqueCount, s);
        return uniqueCount++;
    }

    @Override
    public void commit() throws IOException {
        FileOutputStream sharedStringOutputStream = getSharedStringsTableOutputStream();
        writeTo(sharedStringOutputStream);
        sharedStringOutputStream.close();
    }

    @Override
    public void close() throws Exception {
        stringVsIndexSTMap.clear();
        indexVsStringSTMap.clear();
        stringVsIndexMapDB.close();
        indexVsStringMapDB.close();
    }

    /**
     * Write this table out as XML.
     *
     * @param out The stream to write to.
     * @throws java.io.IOException if an error occurs while writing.
     */
    public void writeTo(OutputStream out) throws IOException {
        //re-create the sst table every time saving a workbook at the end after adding all record using map DB
        try {
            Writer writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
            addDefaultXmlOptions(writer);
            if (uniqueCount != 0) {
                addStringItems(writer);
                addEndDocument(writer);
            }
            writer.flush();
        } catch (XMLStreamException e) {
            throw new RuntimeException("Couldn't write to SharedStringsTable", e);
        }
    }

    private void addDefaultXmlOptions(Writer writer) throws XMLStreamException, IOException {
        writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n");
        String isNoSIElements = uniqueCount == 0 ? "/" : "";
        writer.write("<sst count=\"" + count + "\" uniqueCount=\"" + uniqueCount + "\" xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"" + isNoSIElements + ">");
    }

    private void addStringItems(Writer writer) throws XMLStreamException, IOException {
        for (int i = 0; i < uniqueCount; i++) {
            String s = indexVsStringSTMap.get(i);
            writer.write("<si>");
            writer.write(s);
            writer.write("</si>");
        }
    }

    private void addEndDocument(Writer writer) throws XMLStreamException, IOException {
        writer.write("</sst>");
    }
}
