/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.util.Stack;

/**
 *   
 * @author Racha
 */
public class InfixToPostfix {

    private static boolean isOperator(String c) {
        return c.equalsIgnoreCase("AND") || c.equalsIgnoreCase("OR") || c.equals("(") || c.equals(")");

    }

    private static boolean isLowerPrecedence(String op1, String op2) {
        switch (op1) {
            case "AND":
                
                return (op2 == "AND");

            case "OR":
                System.out.println("Or");
                return (op2 == "OR");
                
            default:
                return false;
        }
    }

    public static String convertToPostfix(String infix) {
        Stack<String> stack = new Stack<String>();
        StringBuffer postfix = new StringBuffer(infix.length());
        String c;
        String[] infixList = infix.split(" ");
        for (int i = 0; i < infixList.length; i++) {
            c = infixList[i];

            if (!isOperator(c)) {
                stack.push(c);
            } else {
                while (!stack.isEmpty() && isLowerPrecedence(c, stack.peek())) {
                    String pop = stack.pop();
                    if (!c.equals("(") || !c.equals(")")) {
                        postfix.append(pop);
                    } else {
                    }
                }
                stack.push(c);
            }
        }
        while (!stack.isEmpty()) {
            postfix.append(stack.pop());
        }
        return "";
    }

    public static void main(String[] args) {
        System.out.println(convertToPostfix("name=bechara OR age<7 AND id=9 OR id=10"));
    }
}
