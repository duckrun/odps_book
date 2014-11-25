package example.util;

import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.Security;

import javax.crypto.Cipher;

/**
 * Copy from 
 * 
 * http://www.blogjava.net/vwpolo/archive/2009/12/05/304874.html
 */
public class DesUtils {
  
  private static String strDefaultKey = "yunti3";

  private Key key;
  private Cipher encryptCipher = null;
  private Cipher decryptCipher = null;

  /**  
   * 将byte数组转换为表示16进制值的字符串， 如：byte[]{8,18}转换为：0813
   *   
   * @param arrB  
   *            需要转换的byte数组  
   * @return 
   *            转换后的字符串  
   */
  private static String byteArr2HexStr(byte[] arrB) {
    int iLen = arrB.length;
    StringBuffer sb = new StringBuffer(iLen * 2);
    for (int i = 0; i < iLen; i++) {
      int intTmp = arrB[i];
      while (intTmp < 0) {
        intTmp = intTmp + 256;
      }
      if (intTmp < 16) {
        sb.append("0");
      }
      sb.append(Integer.toString(intTmp, 16));
    }
    return sb.toString();
  }

  /**  
   * 将表示16进制值的字符串转换为byte数组
   *   
   * @param strIn  
   *            需要转换的字符串  
   * @return 
   *            转换后的byte数组  
   */
  private static byte[] hexStr2ByteArr(String strIn) {
    byte[] arrB = strIn.getBytes();
    int iLen = arrB.length;

    // 两个字符表示一个字节，所以字节数组长度是字符串长度除以2   
    byte[] arrOut = new byte[iLen / 2];
    for (int i = 0; i < iLen; i = i + 2) {
      String strTmp = new String(arrB, i, 2);
      arrOut[i / 2] = (byte) Integer.parseInt(strTmp, 16);
    }
    return arrOut;
  }

  /**  
   * 默认构造方法，使用默认密钥  
   * @throws GeneralSecurityException 
   *   
   * @throws Exception  
   */
  public DesUtils() throws GeneralSecurityException  {
    this(strDefaultKey);
  }

  /**  
   * 指定密钥构造方法  
   *   
   * @param strKey  
   *            指定的密钥，不超过8位 
   * @throws GeneralSecurityException 
   */
  public DesUtils(String strKey) throws GeneralSecurityException {
    Security.addProvider(new com.sun.crypto.provider.SunJCE());
    key = getKey(strKey.getBytes());
  }

  /**  
   * 加密字节数组  
   *   
   * @param arrB  
   *            需加密的字节数组  
   * @return 
   *            加密后的字节数组  
   * @throws GeneralSecurityException 
   */
  private byte[] encrypt(byte[] arrB) throws GeneralSecurityException  {
    if (encryptCipher == null) {
      encryptCipher = Cipher.getInstance("DES");
      encryptCipher.init(Cipher.ENCRYPT_MODE, key);
    }
    return encryptCipher.doFinal(arrB);
  }

  /**  
   * 加密字符串，线程不安全
   *   
   * @param strIn  
   *            需加密的字符串  
   * @return 
   *            加密后的字符串  
   * @throws Exception  
   */
  public String encrypt(String strIn) throws GeneralSecurityException {
    return byteArr2HexStr(encrypt(strIn.getBytes()));
  }

  /**  
   * 解密字节数组  
   *   
   * @param arrB  
   *            需解密的字节数组  
   * @return 解密后的字节数组  
   * @throws GeneralSecurityException  
   */
  private byte[] decrypt(byte[] arrB) throws GeneralSecurityException {
    if (decryptCipher == null) {
      decryptCipher = Cipher.getInstance("DES");
      decryptCipher.init(Cipher.DECRYPT_MODE, key);
    }
    return decryptCipher.doFinal(arrB);
  }

  /**  
   * 解密字符串，线程不安全
   *   
   * @param strIn  
   *            需解密的字符串  
   * @return 解密后的字符串  
   * @throws Exception  
   */
  public String decrypt(String strIn) throws GeneralSecurityException  {
    return new String(decrypt(hexStr2ByteArr(strIn)));
  }

  /**  
   * 从指定字符串生成密钥，密钥所需的字节数组长度为8位， 
   * 不足8位时后面补0，超出8位只取前8位  
   *   
   * @param arrBTmp  
   *            构成该字符串的字节数组  
   * @return 生成的密钥  
   */
  private Key getKey(byte[] arrBTmp) {
    byte[] arrB = new byte[8];

    for (int i = 0; i < arrBTmp.length && i < arrB.length; i++) {
      arrB[i] = arrBTmp[i];
    }

    return new javax.crypto.spec.SecretKeySpec(arrB, "DES");
  }
}