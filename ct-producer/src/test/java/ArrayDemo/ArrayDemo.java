package ArrayDemo;

public class ArrayDemo {

	public static void main(String[] args) {
		int[] array = { 1, 3, 5, 7, 9 };
		int[] array2 = new int[5];
		for (int i = 0; i < array.length; i++) {
			System.out.print(array[i] + " ");
			
		}
		System.out.println();
		for (int j = 0; j < array.length; j++) {
			array2[j] = array[j] + 1;
			System.out.print(array2[j] + " ");
		}
	}
}
