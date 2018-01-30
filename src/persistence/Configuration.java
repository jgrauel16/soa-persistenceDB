public enum Configuration {
    instance;

    public String lineSeparator = System.getProperty("line.separator");
    public String fileSeparator = System.getProperty("file.separator");
    public String dataPath = "data" + fileSeparator;
}